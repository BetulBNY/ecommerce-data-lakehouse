# Hafif bir Python image
FROM python:3.9-slim

# Çalışma klasörüm
WORKDIR /app

# Gereklilikleri kopyalama ve yükleme
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kalan tüm dashboard dosyalarını kopyala
COPY . .

# Streamlit'in standart portu
EXPOSE 8501

# Konteyner başladığında çalışacak komut
# (Adres 0.0.0.0 olmalı ki Docker dışından erişebilelim)
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]