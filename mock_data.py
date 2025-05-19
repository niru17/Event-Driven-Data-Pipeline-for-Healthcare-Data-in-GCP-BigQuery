from datetime import datetime,timedelta
import json
import random

def mock_data(num_rows):
    data=[]
    diseases=[
        ("C15.3", "Esophageal Cancer"),
        ("C20", "Colorectal Cancer"),
        ("C50.9", "Breast Cancer"),
        ("C61", "Prostate Cancer"),
        ("C18.7", "Colon Cancer"),
        ("J18.9", "Pneumonia"),
        ("B20.0", "HIV"),
        ("C22.8", "Liver Cancer"),
        ("J45.9", "Asthma"),
        ("C16.3", "Stomach Cancer")
    ]
    treatments=[
        "Radiation Therapy", "Chemotherapy", "Surgical Resection",
        "Antibiotics", "Targeted Therapy", "Inhaler Therapy", "Hormone Therapy", "Endoscopic Resection"
    ]

    doctors = ["Dr. Alice", "Dr. Bob", "Dr. Charlie", "Dr. Dana", "Dr. Eva", "Dr. Frank", "Dr. Grace", "Dr. Henry", "Dr. Ida", "Dr. John"]
    for i in range(num_rows):
        daig_code,disease = random.choice(diseases)
        row_data={
            "patient_id":f"P{10000 + i}",
            "hospital_id":f"H{500+i}",
            "diagnosis_code":daig_code,
            "disease":disease,
            "diagnosis_date":(datetime.now() - timedelta(days=random.randint(0,3600))).strftime("%Y-%m-%d"),
            "treatment":random.choice(treatments),
            "doctor":random.choice(doctors)
        }
        data.append(row_data)

    with open("mock_clinical_data.json", "w") as f:
        for entry in data:
            json.dump(entry,f)
            f.write("\n")
    print(f"Mock data of {num_rows} generated and saved successfully") 
if __name__ == "__main__":
    mock_data(50)