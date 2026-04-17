import subprocess
import os
import shutil

def git_push_data():
    try:
        git_email = os.getenv("GIT_EMAIL")
        git_user = os.getenv("GIT_USER_NAME")
        git_token = os.getenv("GITHUB_TOKEN")
        git_repo = os.getenv("GITHUB_REPO") 

        # 1. Havada bir klasör açalım (Konteynerin hafızası)
        temp_dir = "/tmp/air_sync"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        # 2. Reponu internetten buraya CLONE et (Geçmişinle birlikte gelir)
        remote_url = f"https://{git_token}@{git_repo}"
        print("Fetching repo from GitHub...")
        subprocess.run(["git", "clone", "--depth", "1", remote_url, temp_dir], check=True)

        # 3. Airflow'un az önce ürettiği CSV'leri bul (Konteyner içindeki adresten)
        # Senin COPY ile oluşturduğun değil, az önce Python'ın yazdığı yer:
        source_csv_path = "/opt/airflow/dashboard/data/"
        target_csv_path = os.path.join(temp_dir, "dashboard/data/")
        
        os.makedirs(target_csv_path, exist_ok=True)
        
        print("Moving newly generated data to sync folder...")
        for file in os.listdir(source_csv_path):
            if file.endswith(".csv"):
                shutil.copy(os.path.join(source_csv_path, file), target_csv_path)

        # 4. Git İşlemleri
        os.chdir(temp_dir)
        subprocess.run(["git", "config", "user.email", git_email], check=True)
        subprocess.run(["git", "config", "user.name", git_user], check=True)
        subprocess.run(["git", "add", "."], check=True)
        
        # Değişiklik var mı?
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout
        if status:
            print("Updates found! Pushing to GitHub...")
            subprocess.run(["git", "commit", "-m", "Auto-update dashboard data [Airflow]"], check=True)
            subprocess.run(["git", "push", "origin", "main"], check=True)
            print("--- PUSH SUCCESSFUL ---")
        else:
            print("No new data to push.")

    except Exception as e:
        print(f"FAILED: {e}")
        raise e