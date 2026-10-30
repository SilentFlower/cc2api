"""独立解码测试 wire，公开输出数值 offer；不导入产品画像或保存恢复材料。"""
import base64
import hashlib
import json
import struct
import sys
from io import BytesIO
from pathlib import Path


def take(stream, size):
    """读取准确字节数。:param stream: 游标。:param size: 字节数。:return: 字节。"""
    value = stream.read(size)
    if len(value) != size:
        raise ValueError('握手向量截断')
    return value


def vector(stream, width):
    """读取长度前缀向量。:param stream: 游标。:param width: 前缀宽度。:return: 字节。"""
    return take(stream, int.from_bytes(take(stream, width), 'big'))


def numbers(payload, prefix, width):
    """读取完整数值向量。:param payload: 原段。:param prefix: 前缀。:param width: 元素宽度。:return: 原序数值。"""
    stream = BytesIO(payload)
    values = vector(stream, prefix)
    assert not stream.read(1) and len(values) % width == 0
    return list(values) if width == 1 else [item[0] for item in struct.iter_unpack('!H', values)]


def parse_body(body):
    """独立解析 ClientHello 本体。:param body: 不含record或handshake头的本体。:return: 无私有材料的字段。"""
    stream = BytesIO(body)
    version = int.from_bytes(take(stream, 2), 'big')
    take(stream, 32)
    session_length = len(vector(stream, 1))
    cipher_bytes = vector(stream, 2)
    assert len(cipher_bytes) % 2 == 0
    ciphers = [item[0] for item in struct.iter_unpack('!H', cipher_bytes)]
    compression = list(vector(stream, 1))
    extension_bytes = vector(stream, 2) if stream.tell() < len(body) else b''
    assert not stream.read(1)
    out = {'legacy_version': version, 'cipher_suites': ciphers, 'session_id_length': session_length,
        'compression_methods': compression, 'signature_algorithms': None, 'supported_groups': None,
        'supported_versions': None, 'ec_point_formats': None, 'key_share': None, 'sni': None,
        'alpn_protocols': None, 'extensions': []}
    extensions = BytesIO(extension_bytes)
    while extensions.tell() < len(extension_bytes):
        kind = int.from_bytes(take(extensions, 2), 'big')
        payload = vector(extensions, 2)
        safe = {'type': kind, 'length': len(payload)}
        if kind in (5, 11, 13, 18, 23, 43, 45, 65281):
            safe['public_payload_hex'] = payload.hex()
        out['extensions'].append(safe)
        if kind in (10, 11, 13, 43):
            name = {10: 'supported_groups', 11: 'ec_point_formats', 13: 'signature_algorithms', 43: 'supported_versions'}[kind]
            out[name] = numbers(payload, 1 if kind in (11, 43) else 2, 1 if kind == 11 else 2)
        elif kind in (0, 16, 51):
            envelope = BytesIO(payload)
            values = vector(envelope, 2)
            assert not envelope.read(1)
            inner = BytesIO(values)
            if kind == 51:
                out['key_share'] = []
            elif kind == 16:
                out['alpn_protocols'] = []
            while inner.tell() < len(values):
                if kind == 0:
                    name_type = take(inner, 1)[0]
                    value = vector(inner, 2)
                    if name_type == 0:
                        out['sni'] = value.decode('ascii')
                elif kind == 16:
                    value = vector(inner, 1)
                    out['alpn_protocols'].append({'text': value.decode('ascii'), 'base64': base64.b64encode(value).decode()})
                else:
                    group = int.from_bytes(take(inner, 2), 'big')
                    out['key_share'].append({'group': group, 'key_exchange_length': len(vector(inner, 2))})
    grease = {0x0a0a + 0x1010 * number for number in range(16)}
    join = lambda values: '-'.join(str(value) for value in values if value not in grease)
    ja3 = ','.join([str(version), join(ciphers), join([item['type'] for item in out['extensions']]),
        join(out['supported_groups'] or []), '-'.join(str(value) for value in out['ec_point_formats'] or [])])
    out['ja3'] = {'string': ja3, 'md5': hashlib.md5(ja3.encode(), usedforsecurity=False).hexdigest()}
    return out


def assert_profile(record, *, resumed=False):
    """对照独立正式基线验证实际字段。:param record: 已解析握手。:param resumed: 是否真实恢复。:return: None。"""
    fixture = json.loads(Path(__file__).with_name('claude_code_293_tls.json').read_text())
    expected = fixture['fingerprint']
    for name, value in expected.items():
        if name == 'ja3':
            continue
        assert record[name] == value, name
    extensions = record['extensions']
    if resumed:
        assert extensions[-1]['type'] == 41
        extensions = extensions[:-1]
    assert extensions == fixture['extensions'], '有序扩展或公开内容'
    digest = fixture['resumption_ja3_md5'] if resumed else expected['ja3']['md5']
    assert record['ja3']['md5'] == digest


def main():
    """从stdin读取完整TLS records并验证首个ClientHello。:return: None，失败退出非零。"""
    wire = BytesIO(sys.stdin.buffer.read())
    handshake = b''
    while wire.tell() < len(wire.getbuffer()):
        header = take(wire, 5)
        assert header[0] == 22
        handshake += take(wire, int.from_bytes(header[3:5], 'big'))
    assert handshake[0] == 1 and len(handshake) == 4 + int.from_bytes(handshake[1:4], 'big')
    record = parse_body(handshake[4:])
    assert_profile(record)
    print(json.dumps({'status': 'passed', 'ja3': record['ja3'], 'fields': record}, ensure_ascii=False))


if __name__ == '__main__':
    main()
