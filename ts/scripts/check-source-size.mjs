import { readdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const sourceDirectory = resolve(process.cwd(), 'src');
const maxLines = 500;
const failures = [];

for (const entry of await readdir(sourceDirectory, { withFileTypes: true })) {
  if (!entry.isFile() || !entry.name.endsWith('.ts')) continue;
  const contents = await readFile(resolve(sourceDirectory, entry.name), 'utf8');
  const newlineCount = contents.match(/\n/g)?.length ?? 0;
  const lines = contents === '' ? 0 : newlineCount + (contents.endsWith('\n') ? 0 : 1);
  if (lines > maxLines) failures.push(`${entry.name}: ${lines} lines (maximum ${maxLines})`);
}

if (failures.length > 0) {
  throw new Error(`TypeScript source files must stay reviewable:\n${failures.join('\n')}`);
}
