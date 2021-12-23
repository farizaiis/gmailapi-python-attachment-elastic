import json

def to_json(file_path):
    raw = open(file_path,"r", errors="ignore").read().splitlines()
    output = []
    for i, e in enumerate(raw):
        if ("row" in e):
            output.append({
                "event_id" : raw[i+1].replace("hex(b.event_id):", "").strip(),
                "data_payload" : {
                    "directive_event:" : raw[i+2].replace("data_payload:", "").replace("directive_event:", "").strip()
                },
                "binary_data" : None if "NULL" in raw[i+3] else raw[i+3].replace("binary_data: ", "").strip()
            })
    with open(file_path, "w") as outfile:
        outfile.write(json.dumps(output))

# to_json('./rawlogs_293F2DFC62C111ECBC6A000C209DBA70.txt')