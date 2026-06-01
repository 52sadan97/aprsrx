FROM python:3.10-slim

# Çalışma dizini oluşturun
WORKDIR /app

# Bağımlılıkları kopyalayıp kurun
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyalayın
COPY . .

# Web sunucusu (6061) ve Özel APRS-IS sunucusu (14581) için portları açın
EXPOSE 6061 14581 14580

# Konteyner başladığında app.py'yi çalıştırın
CMD ["python", "-u", "app.py"]
