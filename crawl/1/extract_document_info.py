import os
import re

def extract_document_info(folder_path):
    code_pattern = r"\d{1,4}/(?:\d{4}/)?[A-ZĐ]{1,5}(?:-[A-Z0-9]{1,5})*"
    agency_pattern = r"(Bộ|Sở|UBND)[^\n\r]+"

    result = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                lines = content.splitlines()

                # 1. Số hiệu
                all_codes = re.findall(code_pattern, content)
                main_code = all_codes[0] if all_codes else None
                related_codes = [c for c in all_codes[1:] if c != main_code]

                # 2. Cơ quan ban hành
                agency = None
                if "|" in content:
                    agency = content.split("|")[1].strip()

                # 3. Ngày ban hành
                issue_date = None
                for line in lines:
                    if "Số:" in line and "ngày" in line.lower():
                        parts = line.split("|")
                        for part in parts:
                            if "ngày" in part.lower():
                                issue_date = part.strip()
                                break
                        if issue_date:
                            break
                if not issue_date:
                    # fallback: tìm dòng nào có "ngày" và "tháng"
                    for line in lines:
                        if re.search(r"ngày\s+\d{1,2}\s+tháng", line.lower()):
                            issue_date = line.strip()
                            break

                # 4. Người ký
                signer = None
                for line in reversed(lines):
                    if re.search(r"\b(KT\.|TL\.|THỨ TRƯỞNG|BỘ TRƯỞNG)\b", line):
                        words = line.strip().split()
                        if len(words) >= 2:
                            signer = " ".join(words[:]) if len(words) >= 3 else " ".join(words[:])
                        break

                result.append({
                    "file": filename,
                    "main_code": main_code,
                    "related_codes": related_codes,
                    "agency": agency,
                    "issue_date": issue_date,
                    "signer": signer
                })

    return result


folder = r"F:\TinHoc\BinningMini\RAG + langchain\crawl\data\tvpl"
docs = extract_document_info(folder)

for doc in docs:
    print(f"\nVăn bản: {doc['file']}")
    print(f"+ Số hiệu chính: {doc['main_code']}")
    print(f"+ Liên quan: {doc['related_codes']}")
    print(f"+ Cơ quan ban hành: {doc['agency']}")
    print(f"+ Nơi ban hành, ngày ban hành: {doc['issue_date']}")
    print(f"+ Người ký: {doc['signer']}")
