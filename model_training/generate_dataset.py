import requests
import pandas as pd
import numpy as np

def fetch_weather_data():
    start_date = '2025-01-01'
    end_date = '2025-12-31'
    
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude=41.0366&longitude=28.8778&start_date={start_date}&end_date={end_date}&hourly=temperature_2m&timezone=Europe%2FIstanbul"
    
    print(f"Hava durumu verisi çekiliyor (2025 Yılı)...")
    response = requests.get(url)
    data = response.json()
    
    if "hourly" not in data:
        raise ValueError("Veri çekilemedi. API Yanıtı: " + str(data))
        
    df_hourly = pd.DataFrame({
        "Tarih_Saat": pd.to_datetime(data["hourly"]["time"]),
        "Sicaklik_C": data["hourly"]["temperature_2m"]
    })
    return df_hourly

def main():
    df_hourly = fetch_weather_data()
    
    # --- GENEL ortalama/std (yıllık) ---
    mean_temp_global = df_hourly["Sicaklik_C"].mean()
    std_temp_global  = df_hourly["Sicaklik_C"].std()
    
    # --- AYLIK ortalama/std (ay bazlı Z-Score için) ---
    df_hourly["Ay"] = df_hourly["Tarih_Saat"].dt.month
    monthly_stats = df_hourly.groupby("Ay")["Sicaklik_C"].agg(["mean", "std"]).rename(
        columns={"mean": "mean_temp", "std": "std_temp"}
    )
    print("\nAylık Sıcaklık İstatistikleri:")
    print(monthly_stats.to_string())
    
    print("\nEdge Computing mimarisi için 30 saniyelik 'Olay (Event)' verileri üretiliyor...")
    
    events_per_hour = 5
    dataset_rows = []
    event_id = 0
    
    for _, row in df_hourly.iterrows():
        base_temp = row["Sicaklik_C"]
        dt = row["Tarih_Saat"]
        saat = dt.hour
        ay = dt.month
        
        # Ay bazlı Z-Score parametreleri
        ay_mean = monthly_stats.loc[ay, "mean_temp"]
        ay_std  = monthly_stats.loc[ay, "std_temp"]
        if ay_std < 1e-6:
            ay_std = 1.0  # sıfıra bölme koruması
        
        for _ in range(events_per_hour):
            for saniye in range(30):
                # ±0.3°C gürültü: gerçek sensörün doğal dağınıklığını simüle eder.
                # Böylece model 26.6–26.8 gibi varyasyonları "normal" olarak öğrenir.
                temp_noise = np.random.normal(0, 0.3)
                anlik_temp = base_temp + temp_noise
                
                # Ay bazlı Z-Score (yıllık değil, o ayın ortalamasına göre)
                z_score = (anlik_temp - ay_mean) / ay_std
                
                kesirli_saat = saat + (dt.minute / 60.0)
                saat_sin = np.sin(2 * np.pi * kesirli_saat / 24.0)
                saat_cos = np.cos(2 * np.pi * kesirli_saat / 24.0)
                
                ay_sin = np.sin(2 * np.pi * ay / 12.0)
                ay_cos = np.cos(2 * np.pi * ay / 12.0)
                
                dataset_rows.append({
                    "Event_ID":        event_id,
                    "Adim":            saniye + 1,
                    "Sicaklik_C":      anlik_temp,
                    "Sicaklik_ZScore": z_score,
                    "Saat_Sin":        saat_sin,
                    "Saat_Cos":        saat_cos,
                    "Ay_Sin":          ay_sin,
                    "Ay_Cos":          ay_cos
                })
            event_id += 1
    
    df_events = pd.DataFrame(dataset_rows)
    output_file = "sensor_events_30sec.csv"
    df_events.to_csv(output_file, index=False)
    
    print(f"\nVeri başarıyla {output_file} dosyasına kaydedildi.")
    print(f"Toplam Üretilen Event (Paket) Sayısı: {event_id}")
    print(f"Toplam Satır Sayısı: {len(df_events)}")
    
    # scaler_params.txt'ye hem global hem de aylık parametreleri yaz
    with open("scaler_params.txt", "w") as f:
        f.write(f"mean_temp={mean_temp_global}\n")
        f.write(f"std_temp={std_temp_global}\n")
        for ay_no in range(1, 13):
            m = monthly_stats.loc[ay_no, "mean_temp"]
            s = monthly_stats.loc[ay_no, "std_temp"]
            f.write(f"mean_temp_ay{ay_no}={m}\n")
            f.write(f"std_temp_ay{ay_no}={s}\n")
    
    print("\nscaler_params.txt güncellendi (aylık istatistikler eklendi).")
    print("\nÖrnek İlk Paket (Event_ID = 0):")
    print(df_events[df_events["Event_ID"] == 0].head())

if __name__ == "__main__":
    main()
