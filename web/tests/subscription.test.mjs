import assert from 'node:assert/strict';
import test from 'node:test';
import { subscriptionLabel, fiveHourLabel } from '../src/lib/subscription.ts';

test('套餐显示使用真实分级，原始 max 和冲突信息不猜倍率', () => {
  for (const [type, tier, expected] of [
    ['ultra', null, 'Ultra'], ['max', 'default_claude_max_20x', 'Ultra'],
    ['max', 'default_claude_max_5x', 'Max'], ['max', null, 'Max'],
    ['pro', 'default_claude_max_20x', 'Pro'], ['team', null, 'Team'],
    ['enterprise', null, 'Enterprise'], [null, null, '套餐未知'], ['future', null, 'future'],
  ]) assert.equal(subscriptionLabel(type, tier), expected);
});

test('5h 空窗口保持空值语义，缺失与异常保留未知，真实零占用不丢失', () => {
  assert.equal(fiveHourLabel({ five_hour: null }, '未知'), '当前未返回5小时窗口');
  assert.equal(fiveHourLabel({}, '未知'), '未知');
  assert.equal(fiveHourLabel({ five_hour: 'invalid' }, '未知'), '未知');
  assert.equal(fiveHourLabel({ five_hour: { utilization: 0 } }, '0%'), '0%');
});
