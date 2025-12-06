#!/usr/bin/env python3
"""
Neo4j Healthcare Demo - Automated Setup Script
==============================================

This script automates the complete setup of the Neo4j healthcare demo:
1. Validates prerequisites (Docker, Java)
2. Generates synthetic patient data using Synthea
3. Starts Neo4j database with Docker Compose
4. Imports patient data (ETL)
5. Computes comorbidity networks
6. Starts Jupyter notebook environment

Platform: Windows, macOS, Linux
Requirements: Docker, Docker Compose, Java 11+
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    @staticmethod
    def disable():
        """Disable colors on Windows if not supported"""
        if platform.system() == 'Windows':
            Colors.HEADER = ''
            Colors.OKBLUE = ''
            Colors.OKCYAN = ''
            Colors.OKGREEN = ''
            Colors.WARNING = ''
            Colors.FAIL = ''
            Colors.ENDC = ''
            Colors.BOLD = ''
            Colors.UNDERLINE = ''


def print_header(text):
    """Print a formatted header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*70}")
    print(f"  {text}")
    print(f"{'='*70}{Colors.ENDC}\n")


def print_success(text):
    """Print success message"""
    print(f"{Colors.OKGREEN}✓ {text}{Colors.ENDC}")


def print_error(text):
    """Print error message"""
    print(f"{Colors.FAIL}✗ {text}{Colors.ENDC}", file=sys.stderr)


def print_warning(text):
    """Print warning message"""
    print(f"{Colors.WARNING}⚠ {text}{Colors.ENDC}")


def print_info(text):
    """Print info message"""
    print(f"{Colors.OKCYAN}ℹ {text}{Colors.ENDC}")


def check_command(command, name):
    """Check if a command is available"""
    result = shutil.which(command)
    if result:
        print_success(f"{name} found: {result}")
        return True
    else:
        print_error(f"{name} not found. Please install {name}.")
        return False


def prompt_input(prompt, default, value_type=str, validation_fn=None):
    """
    Prompt user for input with default value
    
    Args:
        prompt: Question to ask user
        default: Default value if user presses Enter
        value_type: Type to convert input to (str, int, etc.)
        validation_fn: Optional function to validate input
    
    Returns:
        User input or default value
    """
    default_str = str(default)
    while True:
        user_input = input(f"{Colors.OKCYAN}{prompt} [{default_str}]: {Colors.ENDC}").strip()
        
        # Use default if no input
        if not user_input:
            return default
        
        # Convert to desired type
        try:
            value = value_type(user_input)
        except ValueError:
            print_error(f"Invalid input. Expected {value_type.__name__}")
            continue
        
        # Validate if validation function provided
        if validation_fn:
            is_valid, error_msg = validation_fn(value)
            if not is_valid:
                print_error(error_msg)
                continue
        
        return value


def prompt_yes_no(prompt, default=True):
    """
    Prompt user for yes/no question
    
    Args:
        prompt: Question to ask
        default: Default value (True/False)
    
    Returns:
        Boolean value
    """
    default_str = "Y/n" if default else "y/N"
    while True:
        user_input = input(f"{Colors.OKCYAN}{prompt} [{default_str}]: {Colors.ENDC}").strip().lower()
        
        if not user_input:
            return default
        
        if user_input in ['y', 'yes', 'ja', 'j']:
            return True
        elif user_input in ['n', 'no', 'nein']:
            return False
        else:
            print_error("Please answer with y (yes) or n (no)")


def get_system_ram_gb():
    """
    Get system RAM in GB
    
    Returns:
        Total system RAM in GB (int)
    """
    try:
        import psutil
        total_ram_bytes = psutil.virtual_memory().total
        total_ram_gb = int(total_ram_bytes / (1024**3))
        return total_ram_gb
    except ImportError:
        # psutil not available, return None
        return None


def clean_docker_environment(demo_dir, compose_cmd):
    """Clean up Docker containers, volumes, and networks for fresh start"""
    print_header("Cleaning Docker Environment")
    
    compose_file = demo_dir / 'docker-compose.yml'
    
    try:
        # Step 1: Stop and remove all containers from this project
        print_info("Stopping all containers...")
        cmd = compose_cmd.split() + ['-f', str(compose_file), '--profile', 'etl', 'down', '-v']
        subprocess.run(cmd, capture_output=True, text=True)
        print_success("Containers stopped")
        
        # Step 2: Remove specific containers if they still exist (force cleanup)
        containers = ['neo4j-synthea', 'synthea-etl', 'comorbidity-analyzer', 'synthea-notebooks']
        for container in containers:
            result = subprocess.run(
                ['docker', 'rm', '-f', container],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print_info(f"  Removed container: {container}")
        
        # Step 3: Remove the specific network (critical for cache issue fix)
        print_info("Removing synthea-net network...")
        result = subprocess.run(
            ['docker', 'network', 'rm', 'neo4j_demo_synthea_synthea-net'],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print_success("Network removed")
        else:
            # Try alternative network name
            result = subprocess.run(
                ['docker', 'network', 'rm', 'synthea-net'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print_success("Network removed")
        
        # Step 4: Prune unused networks to prevent cache buildup
        print_info("Pruning unused networks...")
        subprocess.run(
            ['docker', 'network', 'prune', '-f'],
            capture_output=True,
            text=True
        )
        print_success("Network cleanup complete")
        
        print_success("Docker environment cleaned")
        return True
        
    except Exception as e:
        print_warning(f"Docker cleanup had issues (non-fatal): {e}")
        return True  # Continue anyway


def get_compatible_java_home():
    """Find a compatible Java version for Synthea (11, 17, or 21 LTS)"""
    # Synthea requires Java 11, 17, or 21 (LTS versions)
    
    # Step 1: Check JAVA_HOME environment variable first
    env_java_home = os.environ.get('JAVA_HOME')
    if env_java_home:
        java_path = Path(env_java_home)
        java_bin = java_path / 'bin' / 'java'
        javac_bin = java_path / 'bin' / 'javac'
        
        if java_bin.exists() and javac_bin.exists():
            try:
                result = subprocess.run(
                    [str(java_bin), '-version'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                version_output = result.stderr
                # Check if it's a compatible version (11, 17, or 21)
                if any(f'version "{v}' in version_output or f'version "{v}.' in version_output 
                       for v in ['11', '17', '21']):
                    print_success(f"Using JAVA_HOME environment variable")
                    print_info(f"  Location: {env_java_home}")
                    print_info(f"  Version: {version_output.split()[2]}")
                    print_info(f"  Compiler: ✓ javac available")
                    return str(env_java_home)
            except:
                pass
    
    # Step 2: Platform-specific Java paths
    compatible_paths = []
    
    if platform.system() == 'Windows':
        # Search for all Java installations with glob patterns
        from glob import glob
        
        # Step 1: Try to use 'where java' command to find Java in PATH
        try:
            result = subprocess.run(
                ['where', 'java'],
                capture_output=True,
                text=True,
                check=True
            )
            # Parse output - each line is a path to java.exe
            for java_exe_path in result.stdout.strip().split('\n'):
                if java_exe_path.strip():
                    # Skip Oracle's javapath wrapper - it's not a real JAVA_HOME
                    if 'javapath' in java_exe_path.lower():
                        continue
                    
                    # Convert java.exe path to JAVA_HOME
                    # C:\...\jdk-17.0.17.10-hotspot\bin\java.exe -> C:\...\jdk-17.0.17.10-hotspot
                    java_path = Path(java_exe_path.strip())
                    if java_path.name.lower() == 'java.exe':
                        potential_home = java_path.parent.parent  # Go up from bin/java.exe
                        if potential_home.exists():
                            home_name = potential_home.name.lower()
                            # Check if it's a compatible version (11, 17, or 21)
                            # Be more flexible with version matching
                            if any(v in home_name for v in ['jdk-21', 'jdk-17', 'jdk-11',
                                                             'jdk21', 'jdk17', 'jdk11',
                                                             '21.', '17.', '11.',
                                                             '-21', '-17', '-11']):
                                if str(potential_home) not in compatible_paths:
                                    compatible_paths.append(str(potential_home))
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        # Step 2: Common installation directories
        base_dirs = [
            'C:\\Program Files\\Java',
            'C:\\Program Files\\OpenJDK',
            'C:\\Program Files\\Amazon Corretto',
            'C:\\Program Files\\Eclipse Adoptium',
            'C:\\Program Files\\Temurin',
            'C:\\Program Files (x86)\\Java',
            'C:\\Program Files (x86)\\OpenJDK',
            'C:\\Program Files (x86)\\Eclipse Adoptium',
        ]
        
        # Search for JDK installations (version 11, 17, 21) with flexible pattern matching
        for base_dir in base_dirs:
            if os.path.exists(base_dir):
                # Find all subdirectories
                for entry in os.listdir(base_dir):
                    full_path = os.path.join(base_dir, entry)
                    if os.path.isdir(full_path):
                        entry_lower = entry.lower()
                        # Check if it's a compatible version (11, 17, or 21)
                        # Support build numbers like jdk-17.0.17.10-hotspot
                        if any(pattern in entry_lower for pattern in [
                            'jdk-21', 'jdk-17', 'jdk-11',
                            'jdk21', 'jdk17', 'jdk11',
                            'temurin-21', 'temurin-17', 'temurin-11',
                            'corretto-21', 'corretto-17', 'corretto-11',
                            'openjdk-21', 'openjdk-17', 'openjdk-11',
                            # Also match build numbers
                            '-21.0.', '-17.0.', '-11.0.',
                            'jdk21.0.', 'jdk17.0.', 'jdk11.0.'
                        ]):
                            if full_path not in compatible_paths:  # Avoid duplicates
                                compatible_paths.append(full_path)
        
        # Sort by version preference: 21 > 17 > 11
        compatible_paths.sort(key=lambda p: (
            '21' in p and -3 or '17' in p and -2 or '11' in p and -1 or 0
        ))
        
    elif platform.system() == 'Darwin':  # macOS
        # macOS has a built-in tool to find Java installations
        # Try using /usr/libexec/java_home for each version
        for version in ['21', '17', '11']:
            try:
                result = subprocess.run(
                    ['/usr/libexec/java_home', '-v', version],
                    capture_output=True,
                    text=True,
                    check=True
                )
                java_home_path = result.stdout.strip()
                if java_home_path and os.path.exists(java_home_path):
                    compatible_paths.append(java_home_path)
            except (subprocess.CalledProcessError, FileNotFoundError):
                pass
        
        # Also search manually in common directories
        jvm_base = Path('/Library/Java/JavaVirtualMachines')
        if jvm_base.exists():
            for entry in jvm_base.iterdir():
                if entry.is_dir():
                    # Check for compatible versions (11, 17, 21)
                    if any(v in entry.name for v in ['jdk-21', 'jdk-17', 'jdk-11',
                                                      'temurin-21', 'temurin-17', 'temurin-11',
                                                      'openjdk-21', 'openjdk-17', 'openjdk-11',
                                                      'zulu21', 'zulu17', 'zulu11',
                                                      'corretto-21', 'corretto-17', 'corretto-11']):
                        home_path = entry / 'Contents' / 'Home'
                        if home_path.exists():
                            compatible_paths.append(str(home_path))
        
        # Homebrew installations
        homebrew_base = Path('/usr/local/opt')
        if homebrew_base.exists():
            for version in ['21', '17', '11']:
                # Check both versioned and unversioned
                for pattern in [f'openjdk@{version}', f'openjdk']:
                    homebrew_path = homebrew_base / pattern
                    if homebrew_path.exists() and homebrew_path.is_symlink():
                        compatible_paths.append(str(homebrew_path))
        
        # Also check /opt/homebrew for Apple Silicon Macs
        homebrew_arm_base = Path('/opt/homebrew/opt')
        if homebrew_arm_base.exists():
            for version in ['21', '17', '11']:
                for pattern in [f'openjdk@{version}', f'openjdk']:
                    homebrew_path = homebrew_arm_base / pattern
                    if homebrew_path.exists() and homebrew_path.is_symlink():
                        compatible_paths.append(str(homebrew_path))
        
        # Sort by version preference: 21 > 17 > 11
        compatible_paths.sort(key=lambda p: (
            '21' in p and -3 or '17' in p and -2 or '11' in p and -1 or 0
        ))
    else:  # Linux
        # Dynamically scan /usr/lib/jvm for compatible JDK installations
        jvm_base = Path('/usr/lib/jvm')
        if jvm_base.exists():
            for entry in jvm_base.iterdir():
                if entry.is_dir():
                    entry_name = entry.name.lower()
                    # Look for JDK (not JRE) of versions 11, 17, or 21
                    # Support different architectures (amd64, arm64, etc.)
                    if any(v in entry_name for v in ['java-21', 'java-17', 'java-11',
                                                      'jdk-21', 'jdk-17', 'jdk-11',
                                                      'temurin-21', 'temurin-17', 'temurin-11',
                                                      'adoptium-21', 'adoptium-17', 'adoptium-11',
                                                      'corretto-21', 'corretto-17', 'corretto-11',
                                                      'zulu21', 'zulu17', 'zulu11']):
                        compatible_paths.append(str(entry))
        
        # Check SDKMAN installations (popular on Linux)
        sdkman_base = Path.home() / '.sdkman' / 'candidates' / 'java'
        if sdkman_base.exists():
            for entry in sdkman_base.iterdir():
                if entry.is_dir() and entry.name != 'current':
                    entry_name = entry.name.lower()
                    # SDKMAN uses naming like: 21.0.1-tem, 17.0.5-zulu, 11.0.17-amzn
                    if any(v in entry_name for v in ['21.', '17.', '11.']):
                        compatible_paths.append(str(entry))
        
        # Check alternatives system (Debian/Ubuntu)
        alternatives_java = Path('/etc/alternatives/java')
        if alternatives_java.exists() and alternatives_java.is_symlink():
            resolved = alternatives_java.resolve()
            # Go up from /usr/lib/jvm/java-XX-openjdk/bin/java to /usr/lib/jvm/java-XX-openjdk
            java_home_candidate = resolved.parent.parent
            if java_home_candidate.exists():
                candidate_name = java_home_candidate.name.lower()
                if any(v in candidate_name for v in ['21', '17', '11']):
                    compatible_paths.append(str(java_home_candidate))
        
        # Sort by version preference: 21 > 17 > 11
        compatible_paths.sort(key=lambda p: (
            '21' in p and -3 or '17' in p and -2 or '11' in p and -1 or 0
        ))
    
    # Step 3: Check each candidate path
    print_info(f"Checking {len(compatible_paths)} potential Java installation(s)...")
    
    for java_home in compatible_paths:
        java_path = Path(java_home)
        java_bin = java_path / 'bin' / 'java'
        javac_bin = java_path / 'bin' / 'javac'  # Check for compiler
        
        # On Windows, also check for .exe extensions
        if platform.system() == 'Windows':
            if not java_bin.exists():
                java_bin = java_path / 'bin' / 'java.exe'
            if not javac_bin.exists():
                javac_bin = java_path / 'bin' / 'javac.exe'
        
        print_info(f"  Checking: {java_home}")
        
        if java_bin.exists():
            # Check if this is a JDK (has compiler) - required for Gradle
            has_compiler = javac_bin.exists()
            
            if not has_compiler:
                print_info(f"    ✗ JRE only (no javac compiler) - skipping")
                continue
            
            try:
                result = subprocess.run(
                    [str(java_bin), '-version'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                version_output = result.stderr
                
                print_success(f"Found compatible Java JDK for Synthea")
                print_info(f"  Location: {java_home}")
                print_info(f"  Version: {version_output.split()[2]}")
                print_info(f"  Compiler: ✓ javac available")
                return str(java_home)
            except Exception as e:
                print_info(f"    ✗ Could not verify Java version: {e}")
                continue
        else:
            print_info(f"    ✗ java executable not found in bin/ directory")
    
    # No JDK found - provide helpful error message
    print_error("No compatible JDK found!")
    print_info("\nSearched locations:")
    for path in compatible_paths[:5]:  # Show first 5
        print_info(f"  • {path}")
    if len(compatible_paths) > 5:
        print_info(f"  ... and {len(compatible_paths) - 5} more")
    
    return None


def check_java_version():
    """Check if a compatible Java version is available for Synthea"""
    try:
        # First try to find a compatible Java installation
        compatible_java = get_compatible_java_home()
        
        if compatible_java:
            print_success(f"Compatible Java found for Synthea (Java 11/17/21)")
            return True
        
        # If no compatible Java found, check default
        result = subprocess.run(
            ['java', '-version'],
            capture_output=True,
            text=True,
            check=True
        )
        version_output = result.stderr
        
        # Extract version number
        version_line = version_output.split('\n')[0]
        print_warning(f"Default Java version: {version_output.split()[2]}")
        
        if 'version "25' in version_line or 'version "22' in version_line:
            print_error("Java 25/22 detected. Synthea requires Java 11, 17, or 21 (LTS)")
            print_error("Please install Java 21 LTS:")
            print_error("  sudo apt install openjdk-21-jdk")
            return False
        
        return True
        
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_error("Java not found. Synthea requires Java 11, 17, or 21 (LTS)")
        return False


def check_docker():
    """Check if Docker is running"""
    try:
        result = subprocess.run(
            ['docker', 'info'],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("Docker is running")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_error("Docker is not running or not installed")
        return False


def check_docker_compose():
    """Check if Docker Compose is available"""
    # Try docker compose (v2)
    try:
        subprocess.run(
            ['docker', 'compose', 'version'],
            capture_output=True,
            check=True
        )
        print_success("Docker Compose (v2) found")
        return 'docker compose'
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Try docker-compose (v1)
    if check_command('docker-compose', 'Docker Compose (v1)'):
        return 'docker-compose'
    
    print_error("Docker Compose not found")
    return None


def update_docker_compose_memory(demo_dir, ram_gb):
    """
    Update docker-compose.yml with RAM configuration
    
    Args:
        demo_dir: Demo directory path
        ram_gb: Total RAM in GB to allocate
    
    Memory allocation strategy:
        - Heap: 50% of total RAM
        - Page Cache: 25% of total RAM
    """
    compose_file = demo_dir / 'docker-compose.yml'
    
    if not compose_file.exists():
        print_warning(f"docker-compose.yml not found at {compose_file}")
        return False
    
    # Calculate memory allocations based on available RAM
    # Conservative approach: ensure total doesn't exceed 80% of allocated RAM
    total_available = int(ram_gb * 0.8)  # Use max 80% to leave room for OS
    
    if total_available >= 12:
        # High memory system (16GB+): Use 8GB heap + 4GB pagecache = 12GB
        heap_gb = 8
        pagecache_gb = 4
    elif total_available >= 6:
        # Medium memory system (8-16GB): Use 4GB heap + 2GB pagecache = 6GB
        heap_gb = 4
        pagecache_gb = 2
    elif total_available >= 3:
        # Low memory system (4-8GB): Use 2GB heap + 1GB pagecache = 3GB
        heap_gb = 2
        pagecache_gb = 1
    else:
        # Very low memory system (<4GB): Use 1GB heap + 512MB pagecache = 1.5GB
        heap_gb = 1
        pagecache_gb = 1
        print_warning(f"Only {ram_gb}GB RAM allocated. Neo4j may run slowly with limited memory.")
    
    total_neo4j = heap_gb + pagecache_gb
    print_info(f"Configuring Neo4j memory: Heap={heap_gb}G, PageCache={pagecache_gb}G (Total: {total_neo4j}G / {ram_gb}G allocated)")
    
    try:
        # Read docker-compose.yml
        with open(compose_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace memory settings using regex
        import re
        
        # Replace heap initial size (handle both quoted and unquoted values)
        content = re.sub(
            r"(NEO4J_dbms_memory_heap_initial__size:\s*)['\"']?[^'\"\n]+['\"']?",
            f"\\1'{heap_gb}G'",
            content
        )
        
        # Replace heap max size
        content = re.sub(
            r"(NEO4J_dbms_memory_heap_max__size:\s*)['\"']?[^'\"\n]+['\"']?",
            f"\\1'{heap_gb}G'",
            content
        )
        
        # Replace page cache size
        content = re.sub(
            r"(NEO4J_dbms_memory_pagecache_size:\s*)['\"']?[^'\"\n]+['\"']?",
            f"\\1'{pagecache_gb}G'",
            content
        )
        
        # Write back
        with open(compose_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print_success(f"Updated docker-compose.yml with memory configuration")
        return True
        
    except Exception as e:
        print_error(f"Failed to update docker-compose.yml: {e}")
        return False


def validate_prerequisites():
    """Validate all prerequisites"""
    print_header("Validating Prerequisites")
    
    checks = {
        'Docker': check_docker(),
        'Docker Compose': check_docker_compose() is not None,
        'Java': check_java_version()
    }
    
    if not all(checks.values()):
        print_error("\nMissing prerequisites. Please install:")
        for name, passed in checks.items():
            if not passed:
                print(f"  - {name}")
        return False
    
    print_success("\nAll prerequisites satisfied")
    return True


def generate_synthea_data(num_patients, state='Massachusetts'):
    """Generate synthetic patient data using Synthea"""
    print_header(f"Generating {num_patients} Synthetic Patients")
    
    synthea_dir = Path(__file__).parent.parent / 'synthea'
    
    if not synthea_dir.exists():
        print_error(f"Synthea directory not found: {synthea_dir}")
        return False
    
    # Check for run_synthea script
    run_script = synthea_dir / 'run_synthea'
    if platform.system() == 'Windows':
        run_script = synthea_dir / 'run_synthea.bat'
    
    if not run_script.exists():
        print_error(f"Synthea run script not found: {run_script}")
        return False
    
    print_info(f"Running Synthea in: {synthea_dir}")
    print_info(f"Generating {num_patients} patients from {state}...")
    
    try:
        # Set JAVA_HOME to a compatible version for Synthea
        env = os.environ.copy()
        
        # Get compatible Java home (11, 17, or 21)
        java_home = get_compatible_java_home()
        
        if java_home:
            env['JAVA_HOME'] = java_home
            # Add Java bin directory to PATH to ensure gradle uses the correct Java
            java_bin = str(Path(java_home) / 'bin')
            # Use platform-specific path separator
            path_separator = ';' if platform.system() == 'Windows' else ':'
            env['PATH'] = f"{java_bin}{path_separator}{env.get('PATH', '')}"
            print_success(f"Using JAVA_HOME: {java_home}")
        else:
            print_error("No compatible Java version found for Synthea")
            print_error("Synthea requires Java 11, 17, or 21 (LTS versions)")
            if platform.system() == 'Linux':
                print_error("Please install: sudo apt install openjdk-21-jdk")
            elif platform.system() == 'Darwin':
                print_error("Please install: brew install openjdk@21")
            elif platform.system() == 'Windows':
                print_error("Please download from: https://adoptium.net/")
            return False
        
        # First, ensure Synthea is built with the correct Java version
        print_info("Building Synthea with compatible Java version...")
        
        # Platform-specific gradle wrapper
        if platform.system() == 'Windows':
            gradle_cmd = 'gradlew.bat'
        else:
            gradle_cmd = './gradlew'
        
        build_cmd = [gradle_cmd, 'build', '-x', 'test', '-Dorg.gradle.java.installations.auto-detect=false']
        build_result = subprocess.run(
            build_cmd,
            cwd=synthea_dir,
            env=env,
            capture_output=True,
            text=True,
            shell=(platform.system() == 'Windows')  # Windows needs shell=True for .bat files
        )
        
        if build_result.returncode != 0:
            print_error("Failed to build Synthea")
            print_error(build_result.stderr)
            return False
        
        print_success("Synthea built successfully")
        
        cmd = [str(run_script), '-p', str(num_patients), state]
        
        print_info(f"Command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            cwd=synthea_dir,
            env=env,
            capture_output=False,
            text=True,
            shell=(platform.system() == 'Windows')  # Windows needs shell=True for .bat files
        )
        
        if result.returncode == 0:
            print_success(f"Generated {num_patients} patients")
            return True
        else:
            print_error(f"Synthea failed with exit code {result.returncode}")
            return False
            
    except Exception as e:
        print_error(f"Failed to run Synthea: {e}")
        return False


def copy_synthea_output(demo_dir):
    """Copy Synthea CSV output to demo import directory"""
    print_header("Copying Synthea Output")
    
    synthea_output = Path(__file__).parent.parent / 'synthea' / 'output' / 'csv'
    import_dir = demo_dir / 'import'
    
    if not synthea_output.exists():
        print_error(f"Synthea output not found: {synthea_output}")
        return False
    
    # Copy CSV files
    csv_files = list(synthea_output.glob('*.csv'))
    if not csv_files:
        print_error("No CSV files found in Synthea output")
        return False
    
    print_info(f"Found {len(csv_files)} CSV files")
    
    # Check if import directory exists and has permission issues
    if import_dir.exists() and not os.access(import_dir, os.W_OK):
        print_warning(f"Import directory exists but no write permission: {import_dir}")
        print_info("Attempting to recreate directory with correct permissions...")
        
        # Try to rename the old directory and create a new one
        backup_dir = import_dir.parent / f"import.backup.{int(time.time())}"
        try:
            import_dir.rename(backup_dir)
            print_info(f"Moved old import directory to: {backup_dir}")
            print_info("You can delete it later with: sudo rm -rf " + str(backup_dir))
        except Exception as e:
            print_error(f"Cannot move old import directory: {e}")
            print_error(f"Please manually fix permissions:")
            print_error(f"  sudo chown -R $USER:$USER {import_dir}")
            print_error(f"Or delete and recreate:")
            print_error(f"  sudo rm -rf {import_dir} && mkdir {import_dir}")
            return False
    
    # Create import directory with correct permissions
    import_dir.mkdir(parents=True, exist_ok=True)
    
    # Verify we have write access
    if not os.access(import_dir, os.W_OK):
        print_error(f"Still cannot write to {import_dir}")
        print_error(f"Please run: sudo chown -R $USER:$USER {import_dir}")
        return False
    
    copied_count = 0
    for csv_file in csv_files:
        dest = import_dir / csv_file.name
        try:
            # Remove existing file if it exists and has wrong permissions
            if dest.exists():
                try:
                    dest.unlink()
                except PermissionError:
                    print_warning(f"Cannot remove existing file: {dest.name}")
                    print_info(f"Attempting to fix permissions for {dest.name}...")
                    try:
                        os.chmod(dest, 0o644)
                        dest.unlink()
                    except:
                        print_error(f"Cannot remove {dest.name}. Please run:")
                        print_error(f"  sudo rm {dest}")
                        continue
            
            # Copy the file
            shutil.copy2(csv_file, dest)
            # Ensure readable permissions
            os.chmod(dest, 0o644)
            print_info(f"  Copied: {csv_file.name}")
            copied_count += 1
            
        except Exception as e:
            print_warning(f"Failed to copy {csv_file.name}: {e}")
            continue
    
    if copied_count == 0:
        print_error("No files could be copied due to permission issues")
        print_error(f"Please run: sudo chown -R $USER:$USER {import_dir}")
        return False
    
    print_success(f"Copied {copied_count}/{len(csv_files)} files to {import_dir}")
    return True


def start_neo4j(demo_dir, compose_cmd, clean_start=False):
    """Start Neo4j with Docker Compose"""
    print_header("Starting Neo4j Database")
    
    compose_file = demo_dir / 'docker-compose.yml'
    
    if not compose_file.exists():
        print_error(f"docker-compose.yml not found: {compose_file}")
        return False
    
    # Clean up if requested (fixes network cache issues)
    if clean_start:
        clean_docker_environment(demo_dir, compose_cmd)
    
    print_info("Starting Docker containers...")
    
    try:
        # Start only neo4j-synthea service (notebooks start separately)
        cmd = compose_cmd.split() + ['-f', str(compose_file), 'up', '-d', 'neo4j-synthea']
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        print_success("Neo4j container started")
        
        # Wait for Neo4j to be ready
        print_info("Waiting for Neo4j to be ready...")
        max_wait = 60
        for i in range(max_wait):
            try:
                result = subprocess.run(
                    ['docker', 'exec', 'neo4j-synthea', 'cypher-shell', 
                     '-u', 'neo4j', '-p', 'synthea123', 'RETURN 1'],
                    capture_output=True,
                    timeout=2
                )
                if result.returncode == 0:
                    print_success("Neo4j is ready")
                    return True
            except:
                pass
            
            if i % 5 == 0:
                print_info(f"  Waiting... ({i}/{max_wait}s)")
            time.sleep(1)
        
        print_warning("Neo4j may not be fully ready. Continuing anyway...")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to start Neo4j: {e}")
        print_error(e.stderr)
        return False


def run_etl(demo_dir, compose_cmd):
    """Run ETL to import data into Neo4j"""
    print_header("Importing Patient Data (ETL)")
    
    compose_file = demo_dir / 'docker-compose.yml'
    
    print_info("Running ETL container...")
    
    try:
        # Use --profile etl to activate the ETL containers
        cmd = compose_cmd.split() + ['-f', str(compose_file), '--profile', 'etl', 'run', '--rm', 'synthea-etl']
        
        # Use Popen with PIPE to handle large output without blocking
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1
        )
        
        # Stream output line by line to prevent buffer overflow
        for line in iter(process.stdout.readline, ''):
            if line:
                print(line, end='', flush=True)
        
        process.wait()
        
        if process.returncode == 0:
            print_success("ETL import completed")
            return True
        else:
            print_error(f"ETL failed with exit code {process.returncode}")
            return False
            
    except subprocess.CalledProcessError as e:
        print_error(f"ETL failed: {e}")
        return False


def compute_comorbidities(demo_dir, compose_cmd):
    """Compute comorbidity networks"""
    print_header("Computing Comorbidity Networks")
    
    compose_file = demo_dir / 'docker-compose.yml'
    
    print_info("Running comorbidity analysis...")
    
    try:
        # Use --profile etl to activate the ETL containers
        cmd = compose_cmd.split() + ['-f', str(compose_file), '--profile', 'etl', 'run', '--rm', 'comorbidity-analyzer']
        
        # Use Popen with PIPE to handle large output without blocking
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1
        )
        
        # Stream output line by line to prevent buffer overflow
        for line in iter(process.stdout.readline, ''):
            if line:
                print(line, end='', flush=True)
        
        process.wait()
        
        if process.returncode == 0:
            print_success("Comorbidity analysis completed")
            return True
        else:
            print_error(f"Comorbidity analysis failed with exit code {process.returncode}")
            return False
            
    except subprocess.CalledProcessError as e:
        print_error(f"Comorbidity analysis failed: {e}")
        return False


def load_drugbank(demo_dir, compose_cmd):
    """Load DrugBank drug interaction data (optional)"""
    print_header("Loading DrugBank Drug Interactions")
    
    # Check if DrugBank data files exist
    drugbank_dir = demo_dir / 'drugData'
    csv_file = drugbank_dir / 'drugbank vocabulary.csv'
    xml_file = drugbank_dir / 'full database.xml'
    
    if not drugbank_dir.exists():
        print_info("DrugBank data directory not found (drugData/)")
        print_info("Skipping DrugBank integration - this is optional")
        return True  # Not an error, just not available
    
    csv_exists = csv_file.exists()
    xml_exists = xml_file.exists()
    
    if not csv_exists and not xml_exists:
        print_info("No DrugBank data files found in drugData/")
        print_info("Skipping DrugBank integration - this is optional")
        return True
    
    # Show what data is available
    if csv_exists:
        csv_size = csv_file.stat().st_size / (1024 * 1024)  # MB
        print_success(f"Found drugbank vocabulary.csv ({csv_size:.1f} MB)")
    if xml_exists:
        xml_size = xml_file.stat().st_size / (1024 * 1024)  # MB
        print_success(f"Found full database.xml ({xml_size:.1f} MB)")
    
    print_info("Running DrugBank ETL pipeline...")
    print_info("This will:")
    if csv_exists:
        print_info("  1. Load ~17,430 drugs from CSV (< 1 min)")
        print_info("  2. Map Synthea medications to DrugBank (< 1 min)")
    if xml_exists:
        print_info("  3. Load ~2.8M drug interactions from XML (10-15 min)")
    print_info("  Total estimated time: 12-17 minutes")
    
    compose_file = demo_dir / 'docker-compose.yml'
    
    try:
        # Use --profile etl to activate the ETL containers
        cmd = compose_cmd.split() + [
            '-f', str(compose_file),
            '--profile', 'etl',
            'run', '--rm',
            '--entrypoint=',  # Override entrypoint to prevent Synthea auto-run
            'synthea-etl',
            'bash', '-c', 
            'pip install --quiet --no-warn-script-location neo4j pandas && python /etl/load_drugbank.py'
        ]
        
        # Use Popen with PIPE to stream output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1
        )
        
        # Stream output line by line
        for line in iter(process.stdout.readline, ''):
            if line:
                print(line, end='', flush=True)
        
        process.wait()
        
        if process.returncode == 0:
            print_success("DrugBank ETL completed successfully")
            print_info("New graph elements:")
            print_info("  • DrugBankDrug nodes (~17k drugs)")
            print_info("  • MAPPED_TO relationships (Medication → DrugBankDrug)")
            print_info("  • INTERACTS_WITH relationships (~2.8M interactions)")
            return True
        else:
            print_error(f"DrugBank ETL failed with exit code {process.returncode}")
            return False
            
    except subprocess.CalledProcessError as e:
        print_error(f"DrugBank ETL failed: {e}")
        return False
    except Exception as e:
        print_error(f"DrugBank ETL failed: {e}")
        return False


def start_jupyter(demo_dir, compose_cmd):
    """Start Jupyter notebook service"""
    print_header("Starting Jupyter Notebook")
    
    compose_file = demo_dir / 'docker-compose.yml'
    
    print_info("Starting Jupyter container...")
    
    try:
        cmd = compose_cmd.split() + ['-f', str(compose_file), 'up', '-d', 'synthea-notebooks']
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        print_success("Jupyter container started")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to start Jupyter: {e}")
        return False


def print_summary(num_patients, drugbank_loaded=False):
    """Print setup summary"""
    print_header("Setup Complete!")
    
    print(f"{Colors.OKGREEN}{Colors.BOLD}")
    print("  ╔════════════════════════════════════════════════════════════╗")
    print("  ║                    Demo is Ready!                          ║")
    print("  ╚════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}")
    
    print(f"\n{Colors.BOLD}Access Points:{Colors.ENDC}")
    print(f"  • Neo4j Browser: {Colors.OKCYAN}http://localhost:7475{Colors.ENDC}")
    print(f"    - Connect to {Colors.FAIL}neo4j://localhost:7688{Colors.ENDC}")
    print(f"    - Username: neo4j")
    print(f"    - Password: synthea123")
    print(f"\n  • Jupyter Notebooks: {Colors.OKCYAN}http://localhost:8889{Colors.ENDC}")
    print(f"    - Token: synthea")
    
    print(f"\n{Colors.BOLD}Generated Data:{Colors.ENDC}")
    print(f"  • Patients: {num_patients}")
    print(f"  • Full healthcare graph with conditions, medications, encounters")
    print(f"  • Comorbidity networks (CO_OCCURS_WITH relationships)")
    print(f"  • Social determinants (RISK_FACTOR_FOR relationships)")
    if drugbank_loaded:
        print(f"  • {Colors.OKGREEN}DrugBank integration: ~17k drugs + 2.8M drug interactions{Colors.ENDC}")
        print(f"    - MAPPED_TO relationships (Medication → DrugBankDrug)")
        print(f"    - INTERACTS_WITH relationships (DrugBankDrug ↔ DrugBankDrug)")
    
    print(f"\n{Colors.BOLD}Next Steps:{Colors.ENDC}")
    print(f"  1. Open Neo4j Browser and explore the graph")
    print(f"  2. Run queries from neo4j/PRESENTATION_STATS.cypher")
    print(f"  3. Open Jupyter notebook: notebooks/01_comorbidity_analysis.ipynb")
    if drugbank_loaded:
        print(f"  4. Check drug interactions: neo4j/04_drug_interactions.cypher")
        print(f"  5. Drug interaction demo: notebooks/04_drug_interaction_checker.ipynb")
        print(f"  6. Documentation: docs/DRUGBANK_SUMMARY.md")
    else:
        print(f"  4. Check documentation: docs/COMORBIDITY_SUMMARY.md")
    
    print(f"\n{Colors.BOLD}Stop Demo:{Colors.ENDC}")
    print(f"  cd neo4j_demo_synthea && docker compose down")
    
    print(f"\n{Colors.BOLD}Clean Restart (fixes network cache issues):{Colors.ENDC}")
    print(f"  python setup.py --patients {num_patients} --clean-start")
    print()


def interactive_configuration():
    """
    Interactive configuration - ask user for all parameters
    
    Returns:
        Dictionary with configuration
    """
    print_header("Neo4j Healthcare Demo - Interactive Setup")
    print(f"{Colors.BOLD}Please answer the following questions to configure your demo.{Colors.ENDC}")
    print(f"{Colors.BOLD}Press Enter to use default values shown in [brackets].{Colors.ENDC}\n")
    
    config = {}
    
    # 1. Number of patients
    def validate_patients(value):
        if value < 25:
            return False, "Minimum 25 patients required for meaningful comorbidity analysis"
        if value > 2500:
            confirmed = prompt_yes_no(
                f"Generating {value} patients may take a long time. Continue?",
                default=False
            )
            if not confirmed:
                return False, "Please choose a smaller number"
        return True, None
    
    config['patients'] = prompt_input(
        "How many synthetic patients to generate?",
        default=100,
        value_type=int,
        validation_fn=validate_patients
    )
    
    # 2. US State for Synthea
    config['state'] = prompt_input(
        "Which US state for Synthea generation?",
        default="Massachusetts",
        value_type=str
    )
    
    # 3. Available RAM
    system_ram = get_system_ram_gb()
    if system_ram:
        print_info(f"Detected system RAM: {system_ram} GB")
        default_ram = min(32, system_ram)  # Default: use up to 32 GB
    else:
        print_warning("Could not detect system RAM automatically")
        default_ram = 16
    
    def validate_ram(value):
        if value < 8:
            return False, "Minimum 8 GB RAM recommended for Neo4j"
        if system_ram and value > system_ram:
            return False, f"Cannot allocate more RAM than available ({system_ram} GB)"
        return True, None
    
    config['ram_gb'] = prompt_input(
        "How much RAM (in GB) to allocate for Neo4j?",
        default=default_ram,
        value_type=int,
        validation_fn=validate_ram
    )
    
    # 4. Skip options
    print(f"\n{Colors.BOLD}Optional: Skip certain steps{Colors.ENDC}")
    
    config['skip_synthea'] = prompt_yes_no(
        "Skip Synthea data generation? (use existing data)",
        default=False
    )
    
    if not config['skip_synthea']:
        config['skip_etl'] = False  # Can't skip ETL if generating new data
    else:
        config['skip_etl'] = prompt_yes_no(
            "Skip ETL import? (database already loaded)",
            default=False
        )
    
    config['skip_drugbank'] = prompt_yes_no(
        "Skip DrugBank drug interaction loading? (optional enhancement)",
        default=True  # Skip by default (optional)
    )
    
    config['no_jupyter'] = not prompt_yes_no(
        "Start Jupyter notebooks after setup?",
        default=True
    )
    
    # 5. Clean start
    config['clean_start'] = prompt_yes_no(
        "Clean Docker environment before starting? (recommended for fresh setup)",
        default=False
    )
    
    # Print configuration summary
    print_header("Configuration Summary")
    print(f"{Colors.BOLD}Your Demo Configuration:{Colors.ENDC}")
    print(f"  • Patients: {config['patients']}")
    print(f"  • State: {config['state']}")
    print(f"  • RAM for Neo4j: {config['ram_gb']} GB")
    print(f"  • Skip Synthea: {'Yes' if config['skip_synthea'] else 'No'}")
    print(f"  • Skip ETL: {'Yes' if config['skip_etl'] else 'No'}")
    print(f"  • Skip DrugBank: {'Yes' if config['skip_drugbank'] else 'No'}")
    print(f"  • Start Jupyter: {'No' if config['no_jupyter'] else 'Yes'}")
    print(f"  • Clean Start: {'Yes' if config['clean_start'] else 'No'}")
    print()
    
    confirmed = prompt_yes_no(
        "Proceed with this configuration?",
        default=True
    )
    
    if not confirmed:
        print_warning("Setup cancelled by user")
        sys.exit(0)
    
    return config


def main():
    """Main setup function"""
    # Disable colors on Windows if needed
    if platform.system() == 'Windows':
        Colors.disable()
    
    # Check if arguments were provided (old CLI mode)
    has_args = len(sys.argv) > 1
    
    if has_args:
        # Legacy mode: Use argparse for backward compatibility
        parser = argparse.ArgumentParser(
            description='Setup Neo4j Healthcare Demo with Synthetic Patient Data',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  python setup.py                       # Interactive mode (recommended)
  python setup.py --patients 100        # Generate 100 patients
  python setup.py --patients 500 --skip-synthea   # Use existing Synthea data
  python setup.py --help                # Show this help
        """
        )
        
        parser.add_argument(
            '--patients',
            type=int,
            default=100,
            help='Number of patients to generate (default: 100, recommended: 100-500)'
        )
        
        parser.add_argument(
            '--state',
            type=str,
            default='Massachusetts',
            help='US state for Synthea generation (default: Massachusetts)'
        )
        
        parser.add_argument(
            '--ram',
            type=int,
            default=None,
            help='RAM in GB to allocate for Neo4j (default: auto-detect, recommended: 16-32)'
        )
        
        parser.add_argument(
            '--skip-synthea',
            action='store_true',
            help='Skip Synthea data generation (use existing data)'
        )
        
        parser.add_argument(
            '--skip-etl',
            action='store_true',
            help='Skip ETL import (database already loaded)'
        )
        
        parser.add_argument(
            '--skip-comorbidity',
            action='store_true',
            help='Skip comorbidity computation (deprecated - now automatic)'
        )
        
        parser.add_argument(
            '--skip-drugbank',
            action='store_true',
            help='Skip DrugBank drug interaction loading (optional enhancement)'
        )
        
        parser.add_argument(
            '--no-jupyter',
            action='store_true',
            help='Do not start Jupyter notebooks'
        )
        
        parser.add_argument(
            '--clean-start',
            action='store_true',
            help='Clean Docker environment before starting (removes containers, networks, volumes)'
        )
        
        args = parser.parse_args()
        
        # Create config from arguments
        config = {
            'patients': args.patients,
            'state': args.state,
            'ram_gb': args.ram,
            'skip_synthea': args.skip_synthea,
            'skip_etl': args.skip_etl,
            'skip_drugbank': args.skip_drugbank,
            'no_jupyter': args.no_jupyter,
            'clean_start': args.clean_start
        }
        
        # Auto-detect RAM if not specified
        if config['ram_gb'] is None:
            system_ram = get_system_ram_gb()
            if system_ram:
                config['ram_gb'] = min(32, system_ram)
                print_info(f"Auto-detected RAM: {system_ram} GB, allocating {config['ram_gb']} GB for Neo4j")
            else:
                # Fallback: Try to read from /proc/meminfo (Linux)
                detected = False
                try:
                    with open('/proc/meminfo', 'r') as f:
                        meminfo = f.read()
                        for line in meminfo.split('\n'):
                            if line.startswith('MemTotal:'):
                                mem_kb = int(line.split()[1])
                                system_ram = int(mem_kb / (1024 * 1024))
                                config['ram_gb'] = min(32, system_ram)
                                print_info(f"Detected RAM from /proc/meminfo: {system_ram} GB, allocating {config['ram_gb']} GB")
                                detected = True
                                break
                except:
                    pass
                
                if not detected:
                    # Use safe default
                    config['ram_gb'] = 8
                    print_warning(f"Could not detect RAM, using safe default: {config['ram_gb']} GB")
                    print_info("For better memory detection, install psutil: pip install psutil")
        
        # Validate patient count
        if config['patients'] < 25:
            print_error("Minimum 25 patients required for meaningful comorbidity analysis")
            return 1
        
        if config['patients'] > 2500:
            print_warning(f"Generating {config['patients']} patients may take a long time")
            response = input("Continue? (y/n): ")
            if response.lower() != 'y':
                return 0
    else:
        # Interactive mode (no arguments provided)
        config = interactive_configuration()
    
    # Get demo directory
    demo_dir = Path(__file__).parent
    
    # Update docker-compose.yml with RAM configuration
    update_docker_compose_memory(demo_dir, config['ram_gb'])
    
    print_header(f"Neo4j Healthcare Demo Setup")
    print(f"{Colors.BOLD}Configuration:{Colors.ENDC}")
    print(f"  • Patients: {config['patients']}")
    print(f"  • State: {config['state']}")
    print(f"  • Neo4j RAM: {config['ram_gb']} GB")
    print(f"  • Demo Directory: {demo_dir}")
    print()    # Step 1: Validate prerequisites
    if not validate_prerequisites():
        return 1
    
    compose_cmd = check_docker_compose()
    
    # Optional: Clean Docker environment before starting
    if config['clean_start']:
        clean_docker_environment(demo_dir, compose_cmd)
    
    # Step 2: Generate Synthea data
    if not config['skip_synthea']:
        if not generate_synthea_data(config['patients'], config['state']):
            print_error("Synthea generation failed")
            return 1
        
        if not copy_synthea_output(demo_dir):
            print_error("Failed to copy Synthea output")
            return 1
    else:
        print_info("Skipping Synthea generation")
    
    # Step 3: Start Neo4j (clean_start flag prevents network cache issues)
    if not start_neo4j(demo_dir, compose_cmd, clean_start=config['clean_start']):
        print_error("Failed to start Neo4j")
        return 1
    
    # Step 4: Run ETL
    if not config['skip_etl']:
        if not run_etl(demo_dir, compose_cmd):
            print_error("ETL failed")
            return 1
    else:
        print_info("Skipping ETL import")
    
    # Step 5: Comorbidity computation is now automatic (via setup_notebooks.py in ETL)
    # Old compute_comorbidities() function is deprecated
    
    # Step 6: Load DrugBank interactions (optional)
    drugbank_loaded = False
    if not config['skip_drugbank']:
        drugbank_loaded = load_drugbank(demo_dir, compose_cmd)
        if not drugbank_loaded:
            print_warning("DrugBank loading failed (non-fatal - this is an optional enhancement)")
    else:
        print_info("Skipping DrugBank integration")
    
    # Step 7: Start Jupyter
    if not config['no_jupyter']:
        if not start_jupyter(demo_dir, compose_cmd):
            print_warning("Jupyter startup failed (non-fatal)")
    else:
        print_info("Skipping Jupyter")
    
    # Print summary
    print_summary(config['patients'], drugbank_loaded)
    
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print_error("\n\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
