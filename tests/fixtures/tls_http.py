"""仅在回环接口提供 TLS 合成响应，证书与私钥均为临时测试资料。"""
import json
import ssl
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Handler(BaseHTTPRequestHandler):
    """验证原始正文、头顺序和 TLS 长流取消，不连接任何外部地址。"""
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        """返回合成额度响应或单片长流，参数为当前请求，返回 None。"""
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        with (self.server.root / "requests.jsonl").open("a") as output:
            output.write(json.dumps({"path": self.path, "headers": [key for key, value in self.headers.raw_items()]}) + "\n")
        if self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(b"9\r\ndata: a\n\n\r\n")
            self.wfile.flush()
            self.connection.settimeout(10)
            try:
                if self.rfile.read(1) == b"":
                    (self.server.root / "stream_closed").touch()
            except TimeoutError:
                pass
            except OSError:
                (self.server.root / "stream_closed").touch()
            self.close_connection = True
            return
        self.send_response(429)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-TLS-Fixture", "verified")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """禁止输出路径、凭据或正文，返回 None。"""
        pass


def main():
    """生成临时 CA/叶证书并启动服务，参数为私有临时目录，返回 None。"""
    root = Path(sys.argv[1])
    commands = [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Local v3 Test CA", "-keyout", "ca.key", "-out", "ca.pem"],
        ["req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=api.anthropic.com", "-keyout", "leaf.key", "-out", "leaf.csr"],
        ["x509", "-req", "-in", "leaf.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-days", "1", "-extfile", "leaf.ext", "-out", "leaf.pem"],
        ["x509", "-in", "ca.pem", "-outform", "DER", "-out", "ca.der"],
    ]
    (root / "leaf.ext").write_text("subjectAltName=DNS:api.anthropic.com\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    for command in commands:
        subprocess.run(["openssl", *command], cwd=root, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.root = root
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(root / "leaf.pem", root / "leaf.key")
    context.set_alpn_protocols(["http/1.1"])
    server.socket = context.wrap_socket(server.socket, server_side=True)
    print(json.dumps({"port": server.server_port}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
