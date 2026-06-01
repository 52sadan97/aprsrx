FROM python:3.10-slim

# Çalışma dizini oluşturun
WORKDIR /app

# Bağımlılıkları kopyalayıp kurun
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyalayın
COPY . .

# Web sunucusu için portu açın (config.json içindeki port)
EXPOSE 14580

# Konteyner başladığında app.py'yi çalıştırın
CMD ["python", "-u", "app.py"]
