"""
Production-сервер Inventory Bot на базе Waitress.

Использование:
    python run_production.py [--host HOST] [--port PORT] [--threads N]

Примеры:
    python run_production.py                     # 0.0.0.0:5000, 8 потоков
    python run_production.py --port 8080
    python run_production.py --host 127.0.0.1 --port 5000 --threads 4
"""
import argparse
import logging
import os
import sys

# Корень проекта в sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger('run_production')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Inventory Bot — production server (waitress)')
    parser.add_argument('--host', default='0.0.0.0', help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='Port (default: 5000)')
    parser.add_argument('--threads', type=int, default=8, help='Number of worker threads (default: 8)')
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        from waitress import serve
    except ImportError:
        print("ERROR: waitress is not installed. Run: pip install waitress", file=sys.stderr)
        sys.exit(1)

    try:
        from app import app
    except Exception as exc:
        print(f"ERROR: Failed to import application: {exc}", file=sys.stderr)
        sys.exit(1)

    # Отключаем режим отладки для production
    app.debug = False

    print(f"Starting Inventory Bot on http://{args.host}:{args.port} ({args.threads} threads)")
    logger.info(f"Starting production server: {args.host}:{args.port}, threads={args.threads}")

    serve(
        app,
        host=args.host,
        port=args.port,
        threads=args.threads,
        channel_timeout=60,
        cleanup_interval=30,
    )


if __name__ == '__main__':
    main()
