import subprocess
import os
from pathlib import Path

def read_version():
    """Read version from version.txt file"""
    with open("version.txt", "r") as f:
        return f.read().strip()

def docker_build_and_publish():
    """Build and publish Docker image to DockerHub"""
    # Configuration
    DOCKER_HUB_USERNAME = os.environ.get("DOCKER_HUB_USERNAME")
    DOCKER_HUB_TOKEN = os.environ.get("DOCKER_HUB_TOKEN")
    
    if not DOCKER_HUB_USERNAME or not DOCKER_HUB_TOKEN:
        raise ValueError("DOCKER_HUB_USERNAME and DOCKER_HUB_TOKEN environment variables must be set")

    try:
        version = read_version()
        image_name = f"{DOCKER_HUB_USERNAME}/deltalens"
        
        print(f"Building Docker image version {version}...")
        
        # Docker login
        subprocess.run([
            "docker", "login",
            "-u", DOCKER_HUB_USERNAME,
            "-p", DOCKER_HUB_TOKEN
        ], check=True)
        
        # Build image
        subprocess.run([
            "docker", "build",
            "-t", f"{image_name}:latest",
            "-t", f"{image_name}:{version}",
            "."
        ], check=True)
        
        # Push images
        print(f"Pushing Docker image {image_name}:{version}...")
        subprocess.run(["docker", "push", f"{image_name}:{version}"], check=True)
        print(f"Pushing Docker image {image_name}:latest...")
        subprocess.run(["docker", "push", f"{image_name}:latest"], check=True)
        
        print("Successfully published Docker images")
        
    except subprocess.CalledProcessError as e:
        print(f"Error executing Docker command: {e}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        # Logout from Docker Hub
        subprocess.run(["docker", "logout"], check=True)

if __name__ == "__main__":
    try:
        docker_build_and_publish()
    except Exception as e:
        print(f"Failed to publish Docker image: {e}")
        exit(1)