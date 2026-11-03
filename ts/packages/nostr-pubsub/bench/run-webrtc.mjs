// Requires the frozen TS workspace and a Chromium executable in BROWSER_BIN.
import { createRequire } from 'node:module';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createServer } from 'node:http';

const here = dirname(fileURLToPath(import.meta.url));
const pkg = resolve(here, '..');
const req = createRequire(join(pkg, 'package.json'));
const vite = await import(pathToFileURL(createRequire(req.resolve('vitest/package.json')).resolve('vite')));
const sizes = (process.env.SIZES || '32,256,1024').split(',').map(Number);
const scenarios = (process.env.SCENARIOS || 'identical,tail-1%,scattered-10%,half,two-sided,disjoint,empty,remote-empty,sparse-local,sparse-remote').split(',');
const modes = (process.env.STRATEGIES || 'inv-want,negentropy').split(',');
const repeats = Number(process.env.REPEATS || 5);
if (!process.env.BROWSER_BIN || !sizes.length || sizes.some(n => !Number.isInteger(n) || n < 4 || n > 4096) ||
    modes.some(mode => !['inv-want','negentropy','negentropy-warm'].includes(mode)) || !Number.isInteger(repeats) || repeats < 1) throw new Error('Set BROWSER_BIN and valid trial bounds');
const output = await vite.build({ root: here, configFile: false, logLevel: 'warn',
  resolve: {alias: {'pubsub-source': join(pkg, 'src'), 'comparison-pubsub': process.env.LEGACY_PUBSUB_ENTRY
      ? resolve(process.env.LEGACY_PUBSUB_ENTRY) : join(pkg, 'src/index.ts'), 'reconcile-source': resolve(pkg, '../nostr-pubsub-reconcile/src/index.ts'),
    'nostr-tools/pure': req.resolve('nostr-tools/pure')}},
  build: {write: false, minify: false, target: 'esnext'} });
const bundle = new Map((Array.isArray(output) ? output : [output]).flatMap(result => result.output)
  .map(item => ['/' + item.fileName, item.type === 'asset' ? item.source : item.code]));
const http = createServer((request, response) => {
  const name = request.url === '/' ? '/index.html' : request.url;
  if (!bundle.has(name)) { response.writeHead(404).end(); return; }
  response.writeHead(200, {'Content-Type': name.endsWith('.html') ? 'text/html' : 'text/javascript'}).end(bundle.get(name));
});
http.listen(0, '127.0.0.1'); await once(http, 'listening');
const profile = await mkdtemp(join(tmpdir(), 'nostr-rtc-bench-'));
let chrome, socket, exit;
try {
  chrome = spawn(process.env.BROWSER_BIN, ['--headless', '--disable-background-networking', '--no-first-run',
    '--disable-extensions', '--allow-loopback-in-peer-connection','--use-fake-device-for-media-stream','--use-fake-ui-for-media-stream', '--disable-features=WebRtcHideLocalIpsWithMdns', '--remote-debugging-port=0', '--user-data-dir=' + profile, 'about:blank'], {stdio: ['ignore','ignore','pipe']});
  exit = once(chrome, 'exit');
  const endpoint = await new Promise((resolve, reject) => {
    let log = '';
    chrome.on('error', reject); chrome.once('exit', () => reject(new Error('Chromium stopped during startup')));
    chrome.stderr.on('data', chunk => { log = (log + chunk).slice(-4096); const url = log.match(/DevTools listening on (ws:\/\/[^\s]+)/); if (url) resolve(url[1]); });
  });
  socket = new WebSocket(endpoint); await once(socket, 'open');
  let seq = 0; const pending = new Map();
  socket.addEventListener('close', () => {
    for (const entry of pending.values()) entry.reject(new Error('Chromium closed'));
    pending.clear();
  });
  socket.onmessage = ({data}) => {
    const message = JSON.parse(data), entry = pending.get(message.id);
    if (!entry) return;
    pending.delete(message.id);
    if (message.error) entry.reject(Error(message.error.message)); else entry.resolve(message.result);
  };
  const call = (method, params = {}, sessionId) => new Promise((resolve,reject) => {
    const id = ++seq; pending.set(id, {resolve,reject}); socket.send(JSON.stringify({id,method,params,sessionId}));
  });
  const {targetId} = await call('Target.createTarget', {url:'about:blank', background:false});
  const {sessionId} = await call('Target.attachToTarget', {targetId, flatten:true});
  const evaluate = async expression => {
    const result = await call('Runtime.evaluate', {expression, returnByValue:true, awaitPromise:true}, sessionId);
    if (result.exceptionDetails) throw Error(result.exceptionDetails.exception?.description || result.exceptionDetails.text);
    return result.result.value;
  };
  await call('Page.navigate', {url:'http://127.0.0.1:' + http.address().port}, sessionId);
  for (let i=0; i<200 && !await evaluate('!!window.bench'); ++i) await new Promise(r => setTimeout(r,50));
  if (!await evaluate('!!window.bench')) throw new Error('Benchmark page failed to initialize');
  const {product} = await call('Browser.getVersion');
  console.log(JSON.stringify({browser:product,sizes,scenarios,modes,repeats,node:process.version}));
  const cpu = async () => (await call('SystemInfo.getProcessInfo')).processInfo.reduce((sum,p) => sum + p.cpuTime, 0);
  const trial = (mode,n,scenario) => evaluate('bench.run(' + [mode,n,scenario].map(v => JSON.stringify(v)).join(',') + ')');
  if (process.env.VERIFY_WARM === '1') {
    console.log(JSON.stringify(await evaluate('bench.verify(' + Boolean(process.env.LEGACY_PUBSUB_ENTRY) + ')')));
  } else for (const n of sizes) {
    await evaluate('bench.prepare(' + n + ')');
    for (const mode of modes) await trial(mode,n,'tail-1%');
    for (let repeat=0; repeat<repeats; ++repeat) for (const [i,scenario] of scenarios.entries()) {
      const offset = (i + repeat) % modes.length;
      for (const mode of [...modes.slice(offset), ...modes.slice(0, offset)]) {
        const before = await cpu();
        const result = await trial(mode,n,scenario);
        console.log(JSON.stringify({...result, repeat, browserCpuMs:Math.round((await cpu()-before)*1000)}));
      }
    }
  }
  await evaluate('bench.close()');
} finally {
  socket?.close(); if (chrome && chrome.exitCode === null) chrome.kill();
  if (exit) await exit.catch(() => {});
  http.close(); await rm(profile, {recursive:true,force:true,maxRetries:5,retryDelay:100});
}
