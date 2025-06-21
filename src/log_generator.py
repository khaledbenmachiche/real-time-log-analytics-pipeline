"""
Web Log Generator for Testing
This script generates realistic Apache access logs for testing the pipeline
"""

import random
import time
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()

class WebLogGenerator:
    """
    Generates realistic web access logs in Apache Common Log Format
    """
    
    def __init__(self):
        self.status_codes = [200, 200, 200, 200, 200, 301, 302, 304, 404, 500]
        self.status_weights = [70, 15, 5, 3, 2, 2, 1, 1, 1, 1]  # Weighted distribution
        
        self.methods = ['GET', 'POST', 'PUT', 'DELETE', 'HEAD']
        self.method_weights = [80, 15, 3, 1, 1]
        
        self.urls = [
            '/', '/index.html', '/about.html', '/contact.html', '/products.html',
            '/api/users', '/api/products', '/api/orders', '/login', '/register',
            '/images/logo.png', '/css/style.css', '/js/app.js', '/favicon.ico',
            '/blog/', '/blog/post-1', '/blog/post-2', '/search', '/admin/',
            '/downloads/', '/documentation/', '/pricing/', '/support/',
            '/missing-page.html', '/error-page.html'  # Some 404 URLs
        ]
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)',
            'Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0'
        ]
        
        # Generate a pool of IP addresses for realistic traffic patterns
        self.ip_pool = [fake.ipv4() for _ in range(100)]
        
        # Some IPs will be more active (simulate real users)
        self.active_ips = random.sample(self.ip_pool, 20)
    
    def generate_ip(self):
        """
        Generate IP address with realistic distribution
        """
        # 30% chance of being an active IP
        if random.random() < 0.3:
            return random.choice(self.active_ips)
        else:
            return random.choice(self.ip_pool)
    
    def generate_status_code(self, url):
        """
        Generate status code based on URL
        """
        # Certain URLs more likely to be 404
        if 'missing' in url or 'error' in url:
            return random.choices([404, 500, 200], weights=[60, 10, 30])[0]
        
        # Admin pages more likely to be 403/401
        if '/admin' in url:
            return random.choices([200, 403, 401], weights=[50, 30, 20])[0]
        
        # Regular status code distribution
        return random.choices(self.status_codes, weights=self.status_weights)[0]
    
    def generate_content_size(self, status_code, url):
        """
        Generate content size based on status code and URL type
        """
        if status_code == 404:
            return random.randint(200, 500)
        elif status_code >= 500:
            return random.randint(100, 300)
        
        # Size based on file type
        if url.endswith('.png') or url.endswith('.jpg'):
            return random.randint(5000, 50000)
        elif url.endswith('.css'):
            return random.randint(1000, 10000)
        elif url.endswith('.js'):
            return random.randint(2000, 20000)
        else:
            return random.randint(500, 5000)
    
    def generate_log_entry(self, timestamp=None):
        """
        Generate a single log entry in Apache Common Log Format
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        ip = self.generate_ip()
        method = random.choices(self.methods, weights=self.method_weights)[0]
        url = random.choice(self.urls)
        protocol = 'HTTP/1.1'
        status_code = self.generate_status_code(url)
        content_size = self.generate_content_size(status_code, url)
        
        # Format timestamp
        timestamp_str = timestamp.strftime('%d/%b/%Y:%H:%M:%S %z')
        if not timestamp_str.endswith(' +0000'):
            timestamp_str = timestamp.strftime('%d/%b/%Y:%H:%M:%S') + ' -0400'
        
        # Apache Common Log Format
        log_entry = f'{ip} - - [{timestamp_str}] "{method} {url} {protocol}" {status_code} {content_size}'
        
        return log_entry
    
    def generate_burst_traffic(self, base_timestamp, duration_minutes=5):
        """
        Generate burst traffic (simulate high load periods)
        """
        logs = []
        end_time = base_timestamp + timedelta(minutes=duration_minutes)
        current_time = base_timestamp
        
        # Higher frequency during burst
        while current_time < end_time:
            # Generate multiple logs per second during burst
            for _ in range(random.randint(5, 15)):
                logs.append(self.generate_log_entry(current_time))
            
            current_time += timedelta(seconds=1)
        
        return logs
    
    def generate_logs_to_file(self, filename, num_logs=10000, time_span_hours=24):
        """
        Generate logs and write to file
        """
        start_time = datetime.now() - timedelta(hours=time_span_hours)
        
        with open(filename, 'w') as f:
            for i in range(num_logs):
                # Generate timestamp within the time span
                random_offset = random.randint(0, int(time_span_hours * 3600))
                timestamp = start_time + timedelta(seconds=random_offset)
                
                # Occasionally generate burst traffic
                if random.random() < 0.05:  # 5% chance of burst
                    burst_logs = self.generate_burst_traffic(timestamp)
                    for log in burst_logs:
                        f.write(log + '\n')
                else:
                    log_entry = self.generate_log_entry(timestamp)
                    f.write(log_entry + '\n')
                
                if (i + 1) % 1000 == 0:
                    print(f"Generated {i + 1} log entries...")
        
        print(f"Generated {num_logs} log entries to {filename}")
    
    def generate_real_time_logs(self, output_file=None, logs_per_second=2):
        """
        Generate logs in real-time for testing streaming
        """
        print(f"Generating real-time logs at {logs_per_second} logs/second...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                log_entry = self.generate_log_entry()
                
                if output_file:
                    with open(output_file, 'a') as f:
                        f.write(log_entry + '\n')
                else:
                    print(log_entry)
                
                time.sleep(1.0 / logs_per_second)
                
        except KeyboardInterrupt:
            print("\nStopped generating logs")


def main():
    """
    Main function for log generation
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Web Log Generator')
    parser.add_argument('--mode', choices=['batch', 'realtime'], default='batch',
                        help='Generation mode: batch or realtime')
    parser.add_argument('--output', '-o', default='generated_logs.txt',
                        help='Output file name')
    parser.add_argument('--count', '-c', type=int, default=10000,
                        help='Number of logs to generate (batch mode)')
    parser.add_argument('--rate', '-r', type=float, default=2.0,
                        help='Logs per second (realtime mode)')
    parser.add_argument('--hours', type=int, default=24,
                        help='Time span in hours for batch generation')
    
    args = parser.parse_args()
    
    generator = WebLogGenerator()
    
    if args.mode == 'batch':
        output_path = os.path.join(os.path.dirname(__file__), '..', args.output)
        generator.generate_logs_to_file(
            output_path, 
            num_logs=args.count,
            time_span_hours=args.hours
        )
    else:
        output_path = os.path.join(os.path.dirname(__file__), '..', args.output) if args.output != 'generated_logs.txt' else None
        generator.generate_real_time_logs(
            output_file=output_path,
            logs_per_second=args.rate
        )


if __name__ == "__main__":
    main()
