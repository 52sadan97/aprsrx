FROM python:3.10-slim

# Çalışma dizini oluşturun
WORKDIR /app

# Zaman dilimini Türkiye (UTC+3) olarak ayarla
ENV TZ="Europe/Istanbul"

# Bağımlılıkları kopyalayıp kurun
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyalayın
COPY . .

# Web sunucusu (14581) ve Özel APRS-IS sunucusu (14580) için portları açın
EXPOSE 14581 14580

# Konteyner başladığında app.py'yi çalıştırın
CMD ["python", "-u", "app.py"]
