import subprocess
import os

def git_push_data():
    """
    Git automation function to push exported CSVs to GitHub.
    This ensures the Streamlit dashboard stays updated automatically.
    """
    try:
        os.chdir("/opt/airflow") # 1. Docker içindeki root klasörüne git
        subprocess.run(["git", "config", "--global", "user.email", "your_email@example.com"], check=True) # 2. Git kimlik bilgilerini ayarla (Her seferinde yapmak güvenlidir)
        subprocess.run(["git", "config", "--global", "user.name", "Airflow Auto-Bot"], check=True)
        subprocess.run(["git", "add", "dashboard/data/*.csv"], check=True) # 3. Değişiklikleri tara ve ekle
        subprocess.run("git commit -m 'Auto-update gold data via Airflow' || true", shell=True, check=True) # 4. Commit oluştur (Hata almamak için değişiklik olup olmadığını kontrol ederiz) 'git commit' eğer değişen bir şey yoksa hata verir, bunu '|| true' ile geçebiliriz
        subprocess.run(["git", "push", "origin", "main"], check=True) # 5. GitHub'a Gönder # Burada 'origin main' ana branch ismi olmalı.
        print("--- GIT UPDATE SUCCESSFUL ---")
        
    except subprocess.CalledProcessError as e:
        print(f"--- GIT ERROR: Process failed with return code {e.returncode} ---")
        raise e # Airflow'un hatayı görmesi için hatayı fırlatıyoruz
    except Exception as e:
        print(f"--- GENERAL ERROR: {e} ---")
        raise e
