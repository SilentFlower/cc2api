"""回环HTTPS夹具：捕获真实握手字段，验证TLS版本、恢复和HRR，无外部请求。"""
import json
import ssl
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tls_clienthello import parse_body


class Handler(BaseHTTPRequestHandler):
    """返回合成正文并记录真实TLS会话状态。"""
    protocol_version = 'HTTP/1.1'

    def do_GET(self):
        """处理本地验证请求。:return: None。"""
        with self.server.lock:
            with (self.server.root / 'requests.jsonl').open('a') as stream:
                stream.write(json.dumps({'path': self.path, 'tls': self.connection.version(),
                    'session_reused': self.connection.session_reused, 'alpn': self.connection.selected_alpn_protocol()}) + '\n')
        body = b'{"synthetic":true}'
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """禁用日志。:param format: 格式。:param args: 参数。:return: None。"""


def main():
    """创建临时CA与本地服务器。:return: None。"""
    root, mode = Path(sys.argv[1]), sys.argv[2]
    commands = [
        ['req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=Synthetic TLS CA','-keyout','ca.key','-out','ca.pem'],
        ['req','-newkey','rsa:2048','-nodes','-subj','/CN=api.anthropic.com','-keyout','leaf.key','-out','leaf.csr'],
        ['x509','-req','-in','leaf.csr','-CA','ca.pem','-CAkey','ca.key','-CAcreateserial','-days','1','-extfile','leaf.ext','-out','leaf.pem'],
        ['x509','-in','ca.pem','-outform','DER','-out','ca.der'],
    ]
    (root / 'leaf.ext').write_text('subjectAltName=DNS:api.anthropic.com\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n')
    for arguments in commands:
        subprocess.run(['openssl', *arguments], cwd=root, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.root, server.lock = root, threading.Lock()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(root / 'leaf.pem', root / 'leaf.key')
    context.set_alpn_protocols(['http/1.1'])
    context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_2 if mode == 'tls12' else ssl.TLSVersion.TLSv1_3
    if mode == 'hrr':
        context.set_ecdh_curve('prime256v1')

    def observe(connection, direction, version, content_type, message_type, data):
        """保存实际完整ClientHello的安全字段。:param connection: TLS连接。:param direction: 方向。:param version: 记录版本。:param content_type: 类型。:param message_type: 消息类型。:param data: 真实握手。:return: None。"""
        if direction == 'read' and int(content_type) == 22 and int(message_type) == 1:
            assert data[0] == 1 and len(data) == 4 + int.from_bytes(data[1:4], 'big')
            record = parse_body(data[4:])
            with server.lock:
                with (root / 'hellos.jsonl').open('a') as stream:
                    stream.write(json.dumps(record) + '\n')

    context._msg_callback = observe
    server.socket = context.wrap_socket(server.socket, server_side=True)
    print(json.dumps({'port': server.server_port}), flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
