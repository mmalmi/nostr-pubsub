import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { relative, resolve } from 'node:path';

const sourceDirectory = fileURLToPath(new URL('../packages/nostr-pubsub/src/', import.meta.url));
const maxLines = 500;
const failures = [];

for (const path of await sourceFiles(sourceDirectory)) {
  const contents = await readFile(path, 'utf8');
  const newlineCount = contents.match(/\n/g)?.length ?? 0;
  const lines = contents === '' ? 0 : newlineCount + (contents.endsWith('\n') ? 0 : 1);
  if (lines > maxLines) {
    failures.push(`${relative(sourceDirectory, path)}: ${lines} lines (maximum ${maxLines})`);
  }
}

if (failures.length > 0) {
  throw new Error(`TypeScript source files must stay reviewable:\n${failures.join('\n')}`);
}

async function sourceFiles(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) files.push(...await sourceFiles(path));
    else if (entry.isFile() && entry.name.endsWith('.ts')) files.push(path);
  }
  return files;
}
