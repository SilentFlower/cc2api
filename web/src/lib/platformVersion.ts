const versionPattern = /^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$/;
const prefix = '平台版本：';
const buildVersion: unknown = import.meta.env.VITE_PLATFORM_VERSION;

/** 当前前端随控制镜像构建的正式平台版本；独立构建缺省保持未知。 */
export const platformVersion = typeof buildVersion === 'string' && versionPattern.test(buildVersion) ? buildVersion : null;

/**
 * 从已验签说明首行提取展示版本。
 * @param changes 已验签的普通文本说明。
 * @return 规范版本；旧发行返回空值。
 */
export function releaseVersion(changes: string): string | null {
  const first = changes.split('\n')[0] ?? '';
  const version = first.startsWith(prefix) ? first.slice(prefix.length) : '';
  return versionPattern.test(version) ? version : null;
}

/**
 * 展示变更正文，正式版本首行由独立标题呈现。
 * @param changes 已验签说明。
 * @return 原说明或去掉受控版本首行的正文。
 */
export function releaseNotes(changes: string): string {
  return releaseVersion(changes) ? changes.slice(changes.indexOf('\n') + 1).trim() : changes;
}
