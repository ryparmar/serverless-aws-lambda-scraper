FROM public.ecr.aws/lambda/python@sha256:d81866a7ab07fb9e725dd4610076462f6eb3b86f2ff771f41bd123127eed6976
# Prepare libraries
RUN yum install atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel wget dpkg -y

# Prepare chrome
ARG CHROME_VERSION="108.0.5359.71"
RUN yum install -y unzip && \
    curl -Lo /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/${CHROME_VERSION}/chromedriver_linux64.zip && \
    curl -Lo /tmp/chrome-linux.zip https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1056889%2Fchrome-linux.zip?alt=media && \
    unzip /tmp/chromedriver.zip -d /opt/ && \
    unzip /tmp/chrome-linux.zip -d /opt/

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY src src