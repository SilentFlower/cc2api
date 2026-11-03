import { test } from 'node:test';
import assert from 'node:assert/strict';
import { accountUpdateDelta } from '../src/lib/accountUpdate.ts';

test('运行中编辑仅发送变化的调度配置，省略认证与身份快照', () => {
  const baseline = {
    auth_type: 'oauth', proxy_url: 'http://synthetic.invalid', account_uuid: 'synthetic',
    organization_uuid: 'synthetic-org', expires_at: '1234567890000',
    setup_token: '', access_token: '', refresh_token: '',
    rpm_limit: 40, concurrency: 4, priority: 49, auto_poll_usage: false,
  };
  assert.deepEqual(accountUpdateDelta({ ...baseline, rpm_limit: 20, concurrency: 2, priority: 50 }, baseline), {
    rpm_limit: 20, concurrency: 2, priority: 50,
  });
  assert.deepEqual(accountUpdateDelta({ ...baseline }, baseline), {});
});

test('明确凭据修改、空值清除和取消输入保持管理接口语义', () => {
  const baseline = { access_token: '', refresh_token: '', expires_at: '123', rate_limit_tier: 'synthetic', name: 'original' };
  assert.deepEqual(accountUpdateDelta({ ...baseline, access_token: 'synthetic-new', refresh_token: ' ', expires_at: '', rate_limit_tier: '', name: '' }, baseline), {
    access_token: 'synthetic-new', expires_at: null, rate_limit_tier: null,
  });
  assert.deepEqual(accountUpdateDelta({ ...baseline, expires_at: ' 0123 ' }, baseline), {});
});
