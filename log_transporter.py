#!/usr/bin/env python3
"""
Log Transporter - Transfers logs from multiple source servers to a destination server
via an intermediate host, avoiding duplicate data using offset tracking.
"""

import os
import sys
import json
import time
import yaml
import logging
import argparse
import hashlib
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import paramiko

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STATE_FILE = '/app/state/transfer_state.json'

@dataclass
class SourceServer:
    name: str
    host: str
    username: str
    ssh_key: str
    log_paths: list
    port: int = 22

@dataclass
class DestServer:
    host: str
    username: str
    ssh_key: str
    base_path: str
    port: int = 22

class TransferState:
    """Tracks file offsets to avoid duplicating log data."""
    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = state_file
        self.state = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load state file: {e}")
        return {}

    def save(self):
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)

    def get_key(self, server_name: str, log_path: str) -> str:
        return hashlib.md5(f"{server_name}:{log_path}".encode()).hexdigest()

    def get_offset(self, server_name: str, log_path: str) -> int:
        key = self.get_key(server_name, log_path)
        return self.state.get(key, {}).get('offset', 0)

    def get_inode(self, server_name: str, log_path: str) -> int:
        key = self.get_key(server_name, log_path)
        return self.state.get(key, {}).get('inode', 0)

    def update(self, server_name: str, log_path: str, offset: int, inode: int):
        key = self.get_key(server_name, log_path)
        self.state[key] = {
            'server': server_name,
            'path': log_path,
            'offset': offset,
            'inode': inode,
            'updated': datetime.utcnow().isoformat()
        }

class SSHConnection:
    """Manages SSH connections with key-based authentication."""
    def __init__(self, host: str, username: str, key_path: str, port: int = 22):
        self.host, self.username, self.key_path, self.port = host, username, key_path, port
        self.client = None

    def _load_key(self, path: str):
        """Load SSH key, handling PKCS#8, OpenSSH, and PEM formats."""
        with open(path, 'rb') as f:
            key_data = f.read()
        
        # Check if it's PKCS#8 format (BEGIN PRIVATE KEY)
        if b'BEGIN PRIVATE KEY' in key_data:
            # Load with cryptography and convert to PEM format Paramiko understands
            private_key = serialization.load_pem_private_key(key_data, password=None, backend=default_backend())
            pem_key = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
            key_file = io.StringIO(pem_key.decode('utf-8'))
            return paramiko.RSAKey.from_private_key(key_file)
        
        # Otherwise let Paramiko handle it directly
        return None

    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        key = self._load_key(self.key_path)
        if key:
            self.client.connect(self.host, port=self.port, username=self.username, pkey=key, timeout=30)
        else:
            self.client.connect(self.host, port=self.port, username=self.username, key_filename=self.key_path, timeout=30)
        
        logger.info(f"Connected to {self.host}")

    def close(self):
        if self.client:
            self.client.close()
            logger.info(f"Disconnected from {self.host}")

    def exec_command(self, cmd: str) -> tuple:
        stdin, stdout, stderr = self.client.exec_command(cmd)
        return stdout.read().decode(), stderr.read().decode(), stdout.channel.recv_exit_status()

    def get_sftp(self):
        return self.client.open_sftp()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

class LogTransporter:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.sources = self._parse_sources()
        self.dest = self._parse_destination()
        self.state = TransferState()
        self.interval = self.config.get('interval', 0)

    def _load_config(self, path: str) -> dict:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _parse_sources(self) -> list:
        return [SourceServer(
            name=s['name'], host=s['host'], username=s['username'],
            ssh_key=s['ssh_key'], log_paths=s['log_paths'], port=s.get('port', 22)
        ) for s in self.config['sources']]

    def _parse_destination(self) -> DestServer:
        d = self.config['destination']
        return DestServer(d['host'], d['username'], d['ssh_key'], d['base_path'], d.get('port', 22))

    def get_file_info(self, ssh: SSHConnection, path: str) -> tuple:
        """Get file size and inode to detect rotation."""
        out, err, code = ssh.exec_command(f"stat -c '%s %i' {path} 2>/dev/null || echo '0 0'")
        parts = out.strip().split()
        return (int(parts[0]), int(parts[1])) if len(parts) == 2 else (0, 0)

    def read_log_chunk(self, ssh: SSHConnection, path: str, offset: int, size: int) -> bytes:
        """Read log data from offset, handling large files efficiently."""
        if offset >= size:
            return b''
        bytes_to_read = size - offset
        out, err, code = ssh.exec_command(f"tail -c +{offset + 1} {path} | head -c {bytes_to_read}")
        return out.encode() if isinstance(out, str) else out

    def transfer_logs(self):
        """Main transfer logic - connects to sources, reads new data, writes to destination."""
        logger.info("Starting log transfer cycle")
        
        with SSHConnection(self.dest.host, self.dest.username, self.dest.ssh_key, self.dest.port) as dest_ssh:
            dest_sftp = dest_ssh.get_sftp()
            
            # Ensure base path exists
            try:
                dest_sftp.stat(self.dest.base_path)
            except FileNotFoundError:
                dest_ssh.exec_command(f"mkdir -p {self.dest.base_path}")

            for source in self.sources:
                logger.info(f"Processing source: {source.name} ({source.host})")
                server_dest_dir = f"{self.dest.base_path}/{source.name}"
                dest_ssh.exec_command(f"mkdir -p {server_dest_dir}")

                try:
                    with SSHConnection(source.host, source.username, source.ssh_key, source.port) as src_ssh:
                        for log_path in source.log_paths:
                            try:
                                self._transfer_single_log(src_ssh, dest_ssh, dest_sftp, source, log_path, server_dest_dir)
                            except Exception as e:
                                logger.error(f"Error transferring {log_path}: {type(e).__name__}: {e}")
                except Exception as e:
                    logger.error(f"Failed to connect to {source.name}: {e}")
                    continue

            dest_sftp.close()
        
        self.state.save()
        logger.info("Transfer cycle complete")

    def _transfer_single_log(self, src_ssh, dest_ssh, dest_sftp, source, log_path, dest_dir):
        """Transfer a single log file, handling rotation and deduplication."""
        log_name = os.path.basename(log_path)
        dest_file = f"{dest_dir}/{log_name}"
        
        current_size, current_inode = self.get_file_info(src_ssh, log_path)
        if current_size == 0:
            logger.warning(f"Log file not found or empty: {log_path}")
            return

        saved_offset = self.state.get_offset(source.name, log_path)
        saved_inode = self.state.get_inode(source.name, log_path)

        # Detect log rotation (inode changed or file smaller than offset)
        if saved_inode != current_inode or current_size < saved_offset:
            logger.info(f"Log rotation detected for {log_path}, resetting offset")
            saved_offset = 0

        if saved_offset >= current_size:
            logger.info(f"No new data in {log_path} (offset {saved_offset} >= size {current_size})")
            return

        # Read new log data from source
        new_data, _, _ = src_ssh.exec_command(f"tail -c +{saved_offset + 1} '{log_path}'")
        if not new_data:
            return

        new_bytes = len(new_data.encode())
        logger.info(f"Transferring {new_bytes} bytes from {source.name}:{log_path}")

        # Ensure destination directory exists
        dest_ssh.exec_command(f"mkdir -p '{dest_dir}'")
        logger.info(f"Ensured directory exists: {dest_dir}")
        
        # Get current size of destination file (0 if doesn't exist)
        dest_out, _, _ = dest_ssh.exec_command(f"stat -c '%s' '{dest_file}' 2>/dev/null || echo '0'")
        dest_size = int(dest_out.strip())
        logger.info(f"Destination file size: {dest_size}, writing to: {dest_file}")
        
        # Write using SFTP - open in read/write mode, seek to end
        mode = 'r+b' if dest_size > 0 else 'wb'
        try:
            with dest_sftp.open(dest_file, mode) as f:
                if dest_size > 0:
                    f.seek(0, 2)  # Seek to end
                f.write(new_data.encode() if isinstance(new_data, str) else new_data)
            logger.info(f"Successfully wrote to {dest_file}")
        except IOError as e:
            logger.warning(f"IOError on first attempt: {e}, trying fallback")
            # Fallback: create new file
            with dest_sftp.open(dest_file, 'wb') as f:
                f.write(new_data.encode() if isinstance(new_data, str) else new_data)
            logger.info(f"Successfully wrote to {dest_file} via fallback")

        self.state.update(source.name, log_path, current_size, current_inode)

    def run(self, continuous: bool = False):
        """Run transfer once or continuously based on interval."""
        if continuous and self.interval > 0:
            logger.info(f"Running in continuous mode with {self.interval}s interval")
            while True:
                try:
                    self.transfer_logs()
                except Exception as e:
                    logger.error(f"Transfer cycle failed: {e}")
                time.sleep(self.interval)
        else:
            self.transfer_logs()

def main():
    parser = argparse.ArgumentParser(description='Transfer logs between servers')
    parser.add_argument('-c', '--config', default='/app/config/config.yaml', help='Config file path')
    parser.add_argument('--continuous', action='store_true', help='Run continuously')
    args = parser.parse_args()

    if not os.path.exists(args.config):
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)

    transporter = LogTransporter(args.config)
    transporter.run(continuous=args.continuous)

if __name__ == '__main__':
    main()
