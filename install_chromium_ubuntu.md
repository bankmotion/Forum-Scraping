# Chromium Installation Guide for Ubuntu

## 1. Enable the official Chromium packages

On Ubuntu 22.04+ Chromium defaults to Snap, but you can still get the .deb builds from the Ubuntu Chromium Team PPA.

```bash
sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:saiarcot895/chromium-beta
sudo apt update
sudo apt install -y chromium-browser
```

This installs Chromium from a .deb package (maintained in that PPA), and the binary path will be:

```
/usr/bin/chromium-browser
```

## 2. Verify install

```bash
which chromium-browser
chromium-browser --version
```

You should see something like:

```
/usr/bin/chromium-browser
Chromium 126.0.xxxx.x
```

## 3. Update your Puppeteer config

Point Puppeteer at the installed binary:

```javascript
const browser = await puppeteer.launch({
  executablePath: '/usr/bin/chromium-browser',
  args: ['--no-sandbox','--disable-setuid-sandbox'],
});
```

✅ That will give you a proper .deb-based Chromium installation instead of the Snap one.

## 4. Remove Snap Chromium (Optional)

If you already have Snap Chromium installed and want to remove it:

```bash
# Remove Snap Chromium
sudo snap remove chromium

# Remove Snap Chromium from snap store cache
sudo snap remove chromium --purge

# Verify removal
snap list | grep chromium
```

## 5. Alternative Installation Methods

### Method 1: Direct Download
```bash
# Download latest Chromium .deb package
wget https://download-chromium.appspot.com/dl/Linux_x64?type=snapshots
```

### Method 2: Using Chromium Team PPA (Recommended)
```bash
# Add Chromium Team PPA
sudo add-apt-repository ppa:saiarcot895/chromium-beta
sudo apt update
sudo apt install chromium-browser
```

### Method 3: Manual Installation
```bash
# Download and install manually
cd /tmp
wget https://download-chromium.appspot.com/dl/Linux_x64?type=snapshots -O chromium.tar.xz
tar -xf chromium.tar.xz
sudo mv chromium-linux-* /opt/chromium
sudo ln -s /opt/chromium/chrome /usr/bin/chromium-browser
```

## 6. Troubleshooting

### Check if Chromium is properly installed:
```bash
# Check binary location
which chromium-browser

# Check version
chromium-browser --version

# Check if it's a snap or deb package
dpkg -l | grep chromium
```

### If you get permission errors:
```bash
# Fix permissions
sudo chmod +x /usr/bin/chromium-browser
```

### If Puppeteer still can't find Chromium:
```bash
# Check if the path exists
ls -la /usr/bin/chromium-browser

# Test manual launch
/usr/bin/chromium-browser --version
```

## 7. Complete Installation Script

Here's a complete script that handles everything:

```bash
#!/bin/bash

echo "Installing Chromium for Puppeteer..."

# Remove Snap Chromium if exists
if snap list | grep -q chromium; then
    echo "Removing Snap Chromium..."
    sudo snap remove chromium
fi

# Add Chromium PPA
echo "Adding Chromium PPA..."
sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:saiarcot895/chromium-beta -y
sudo apt update

# Install Chromium
echo "Installing Chromium..."
sudo apt install -y chromium-browser

# Verify installation
echo "Verifying installation..."
which chromium-browser
chromium-browser --version

echo "Installation complete!"
echo "Chromium binary location: $(which chromium-browser)"
```

Save this as `install_chromium.sh` and run:
```bash
chmod +x install_chromium.sh
./install_chromium.sh
```

## 8. Environment Variables

You can also set environment variables for Puppeteer:

```bash
# Add to your .bashrc or .profile
export PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium-browser
export PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
```

This way, Puppeteer will automatically use the system Chromium installation. 