import csv
import re


def log_to_dict(file_path: str) -> dict:
    """Parses through lines of log to retrieve latency data. Returns a
    dictionaries mapping a file ID to a list of dictionaries containing
    the time of an event and the event.

    Parameters
    ----------
    file_path : str
        Path to log file to parse.

    Returns
    -------
    event_map : dict
        Mapping of file ID to a list of dictionaries containing
    the time of an event and the event
    """
    log_lines = []
    with open(file_path) as f:
        for line in f.readlines():
            log_lines.append(line.rstrip())

    event_map = dict()
    search_string = "LATENCY PLACING ([0-9a-z\-]+) INTO ([a-zA-Z]*?) AT ([0-9.]+)"
    for line in log_lines:
        search_result = re.search(search_string, line)

        if not search_result:
            continue

        file_id = search_result.group(1)
        event = search_result.group(2)
        time = float(search_result.group(3))

        if file_id in event_map:
            event_map[file_id].append({"event": event, "time": time})
        else:
            event_map[file_id] = [{"event": event, "time": time}]

    return event_map


def event_map_to_csv(event_map: dict, csv_path: str) -> None:
    rows = []

    for file_id, events in event_map.items():
        events.sort(key=(lambda x: x["time"]))

        idx = 0
        while idx < len(events) - 1:
            event_1 = events[idx]["event"]
            time_1 = events[idx]["time"]

            event_2 = events[idx + 1]["event"]
            time_2 = events[idx + 1]["time"]

            if event_2 == "FAILED":
                idx += 1
                continue
            elif event_2 == "UNPREDICTED":
                if event_1 == "DECOMPRESSING":
                    event = "decompressing"
                elif event_1 == "CONSOLIDATING":
                    event = "resubmitting"
            elif event_2 == "PREDICTED":
                event = "predicting"
            elif event_2 == "SCHEDULED":
                event = "scheduling"
            elif event_2 == "PREFETCHING":
                event = "requesting_prefetch"
            elif event_2 == "PREFETCHED":
                event = "prefetching"
            elif event_2 == "DECOMPRESSING":
                event = "requesting_decompress"
            elif event_2 == "CRAWLING":
                event = "requesting_crawl"
            elif event_2 == "DECOMPRESSED":
                event = "decompressing"
            elif event_2 == "DECOMPRESSING":
                event = "requesting_decompress"
            elif event_2 == "CONSOLIDATING":
                event = "crawling"
            elif event_2 == "DECOMPRESSING":
                event = "requesting_decompress"
            elif event_2 == "SUCCEEDED":
                event = "consolidating"

            rows.append({"file_id": file_id, "event": event, "time": time_2 - time_1})
            idx += 1
    return rows

    # with open(csv_path, mode='w') as csv_file:
    #     fieldnames = ["file_id", "event", "time"]
    #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    #
    #     writer.writeheader()
    #
    #     for row in rows:
    #         writer.writerow(row)


if __name__ == "__main__":
    x = log_to_dict("/Users/ryan/Documents/CS/abyss/abyss/file.log")
    y = event_map_to_csv(x, "")
    print(x)
    print(y)




