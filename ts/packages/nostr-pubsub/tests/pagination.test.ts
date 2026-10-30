import { expect, it } from 'vitest';
import { finalizeEvent } from 'nostr-tools/pure';
import { HistoryCursor } from '../src/pagination.js';
import { relaySource } from '../src/source.js';
import { verifyNostrEvent } from '../src/types.js';
const secret = new Uint8Array(32).fill(17);
const delivery = (source: string, content: string, at: number) => ({event:verifyNostrEvent(finalizeEvent({kind:1,content,created_at:at,tags:[]},secret)),source:relaySource(source),priority:0});
const cursor = (source='a', limit=3, max=12) => new HistoryCursor(source,{kinds:[1],limit},1000,max);
it('retains incomplete boundaries and exposes observations without claiming exhaustion',()=>{
  const c=cursor(), page=c.beginPage()!;c.observe(page.token,delivery('a','partial',800));
  expect(c.finishPage(page.token,'partial')).toEqual({status:'retry',oldest:800,uniqueEvents:1});
  expect(c.beginPage()!.filter.until).toBe(1000);
});
it('advances a short complete page inclusively',()=>{
  const c=cursor('a',300,1200), page=c.beginPage()!;c.observe(page.token,delivery('a','short',873));
  expect(c.finishPage(page.token,'complete').status).toBe('advanced');
  expect(c.beginPage()!.filter).toMatchObject({until:873,limit:300});
});
it('keeps per-source cursors and duplicate deliveries independent',()=>{
  const a=cursor('a'), b=cursor('b'), pa=a.beginPage()!, pb=b.beginPage()!, event=delivery('a','same',900);
  expect(a.observe(pa.token,event)).toBe(true);expect(a.observe(pa.token,event)).toBe(false);
  expect(b.observe(pb.token,event)).toBe(false);expect(b.observe(pb.token,{...event,source:relaySource('b')})).toBe(true);
  b.observe(pb.token,delivery('b','outlier',100));a.finishPage(pa.token,'complete');b.finishPage(pb.token,'complete');
  expect(a.beginPage()!.filter.until).toBe(900);expect(b.beginPage()!.filter.until).toBe(100);
});
it('grows dense seconds before stopping at an explicit bound',()=>{
  const c=cursor('a',2,4);let p=c.beginPage()!;
  for(const id of ['a','b'])c.observe(p.token,delivery('a',id,1000));c.finishPage(p.token,'complete');
  p=c.beginPage()!;expect(p.filter.limit).toBe(4);
  for(const id of ['a','b','c','d'])c.observe(p.token,delivery('a',id,1000));
  expect(c.finishPage(p.token,'complete').status).toBe('limited');expect(c.beginPage()).toBeUndefined();
});
it('only complete unsaturated boundary can decrement the second',()=>{
  const c=cursor('a',2,4),p=c.beginPage()!;c.observe(p.token,delivery('a','boundary',1000));c.finishPage(p.token,'complete');
  expect(c.beginPage()!.filter.until).toBe(999);
});
it('honors since/until and ends complete empty pages',()=>{
  const c=new HistoryCursor('a',{since:1000,limit:2},1000,4),p=c.beginPage()!;
  expect(c.observe(p.token,delivery('a','old',999))).toBe(false);expect(c.observe(p.token,delivery('a','future',1001))).toBe(false);
  c.observe(p.token,delivery('a','boundary',1000));expect(c.finishPage(p.token,'complete').status).toBe('exhausted');
  const empty=cursor(),ep=empty.beginPage()!;expect(empty.finishPage(ep.token,'complete').status).toBe('exhausted');
});
it('rejects stale tokens and cancellation',()=>{
  const c=cursor(),p=c.beginPage()!;expect(c.beginPage()).toBeUndefined();c.finishPage(p.token,'partial');const active=c.beginPage()!;
  expect(c.observe(p.token,delivery('a','late',800))).toBe(false);expect(c.finishPage(p.token,'complete').status).toBe('stale');
  c.cancel();expect(c.observe(active.token,delivery('a','late',800))).toBe(false);expect(c.beginPage()).toBeUndefined();
});
it('rejects invalid ranges and limits, and bounds retained IDs',()=>{
  for(const args of [[{limit:0},4],[{limit:5},4],[{since:1001},100]] as const)expect(()=>new HistoryCursor('a',args[0],1000,args[1])).toThrow();
  const c=cursor('a',2,2),p=c.beginPage()!;for(const id of ['a','b','c'])c.observe(p.token,delivery('a',id,900));
  expect(c.finishPage(p.token,'partial')).toMatchObject({status:'limited',uniqueEvents:2});
});

it('binds tokens to one cursor instance even with identical first pages',()=>{
  const old=cursor(),fresh=cursor(),oldPage=old.beginPage()!,page=fresh.beginPage()!;
  expect(oldPage.token).not.toBe(page.token);
  expect(fresh.observe(oldPage.token,delivery('a','late query',900))).toBe(false);
  expect(fresh.finishPage(oldPage.token,'complete').status).toBe('stale');
  expect(fresh.observe(page.token,delivery('a','new query',900))).toBe(true);
});
