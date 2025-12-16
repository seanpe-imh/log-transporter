# Log Transporter

A dockerized application that transfers log files from multiple source servers to a destination server via an intermediate host, with built-in deduplication.

## Directory Structure

```
log-transporter/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── log_transporter.py
├── config/
│   └── config.yaml
├── keys/
│   ├── source1_key
│   └── dest_key
└── state/
    └── transfer_state.json  (auto-generated)
```

## Setup

### 1. Create the directory structure

```bash
mkdir -p log-transporter/{config,keys,state}
cd log-transporter
```

### 2. Copy your SSH keys

```bash
cp /path/to/source1_key ./keys/source1_key
cp /path/to/dest_key ./keys/dest_key
chmod 600 ./keys/*
```

### 3. Edit the configuration

Edit `config/config.yaml` with your server details.

### 4. Build and run

```bash
docker-compose build
docker-compose up -d
docker-compose logs -f
```

## Deduplication Method

The application avoids duplicating log data using:

1. **Byte Offset Tracking**: Stores the last read position for each log file
2. **Inode Monitoring**: Detects log rotation by tracking file inodes
3. **Rotation Handling**: Resets offset when a log file is rotated
4. **Persistent State**: Maintains state across restarts

## Destination File Structure

```
/logs/collected/
├── web-server-1/
│   ├── access.log
│   └── error.log
├── app-server-1/
│   └── app.log
└── db-server-1/
    └── postgresql.log
```
