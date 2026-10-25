# Full Dataset Streaming — FreeManufacturingQC100M

## Mục đích

`simulation_iot_full_100m.py` phát lại **từng dòng** của file Parquet gốc lên Azure IoT Hub (một dòng = một telemetry JSON). Không lấy mẫu, không tạo ba pha giả lập, không nạp toàn bộ 100 triệu dòng vào RAM.

Pipeline giữ nguyên:

`Hugging Face Parquet → Azure VM (PyArrow batch reader + bounded asyncio queue) → IoT Hub → Azure Stream Analytics (SAquerry_calibrated.sql) → Blob Storage: trends/, alerts/ → Power BI`.

Cần phân biệt **100 triệu dòng dữ liệu nguồn**, **số JSON được SDK xác nhận gửi**, **Input Events của ASA**, **số output của ASA** và **số byte lưu ở Blob**: năm con số này không nhất thiết bằng nhau.

## Thay đổi so với `simulation_iot_optimize.py`

| Thành phần | Phiên bản cũ | Full Dataset Streaming |
|---|---|---|
| Đọc Parquet | Batch + giới hạn số dòng quét | Batch đến EOF, không đặt giới hạn mặc định |
| Chọn dữ liệu | Stratified Reservoir Sampling (40/30/30) | Không lấy mẫu, giữ phân bố dataset gốc |
| Máy logic | 5 | 5 mặc định, `--machines N` |
| Lịch phát | Normal → Anomaly → Recovery | Đọc theo thứ tự dòng Parquet, nhưng thứ tự nhận có thể thay đổi khi gửi song song |
| Tốc độ gửi | Mỗi máy ngủ 1 giây | **Global rate limiter**, `--rate` đơn vị attempts/giây |
| IoT identities | Một | Một hoặc nhiều DeviceId **riêng biệt** để tăng khả năng gửi song song |
| RAM | Giới hạn số records được chọn | `--batch-size` + `--queue-size` hữu hạn, không giữ toàn bộ dữ liệu |
| Resume | Không | JSON checkpoint trên VM, `--resume`, *at-least-once* |
| Log | Mean/P50/P95 trên list độ trễ | Mean chính xác của các mẫu đo; P50/P95 **xấp xỉ** từ histogram logarithmic |

## 1. Chuẩn bị máy ảo Ubuntu 24.04

```bash
sudo apt update
sudo apt install -y python3-venv python3-pip
python3 -m venv ~/iot-venv
source ~/iot-venv/bin/activate
python -m pip install -r requirements_iot_full_100m.txt
```

Dùng Python 3.11+ vì script sử dụng `asyncio.TaskGroup`.

**Khuyến nghị tải bản Parquet về đĩa/Blob gắn trên VM** nếu mạng đọc `hf://` bị chập chờn. Khi thay `--source`, luôn dùng cùng path lúc `--resume`; checkpoint mặc định ràng buộc với chuỗi source, số dòng và schema. Để tái lập chính xác, có thể dùng revision Hugging Face cố định.

## 2. Kiểm thử không gửi Azure

```bash
python simulation_iot_full_100m.py \
  --dry-run \
  --limit 1000 \
  --rate 0 \
  --checkpoint chk_dry.json \
  --summary sum_dry.json
```

Chế độ này **không kết nối IoT Hub** và không đo SDK latency. `--limit 1000` nghĩa là chỉ xử lý nguồn đến row index 999 (không phải quota cho từng lần resume). Xem vài payload mẫu, `sum_dry.json` và `chk_dry.json`.

Tiếp tục dry run qua dữ liệu còn lại (nếu muốn):

```bash
python simulation_iot_full_100m.py --dry-run --rate 0 \
  --resume --checkpoint chk_dry.json --summary sum_dry.json
```

Không được dùng checkpoint đã tạo trong `--dry-run` để tiếp tục gửi thật. Hãy dùng đường dẫn checkpoint riêng.

## 3. Kết nối IoT Hub: một device identity

Trước tiên phải có **IoT Hub trả phí đủ quota**, một registered device và Azure Stream Analytics Job đang chạy. Không dùng khóa từng xuất hiện trong source cũ; regenerate khóa nếu từng bị lộ. Không đưa connection strings vào Git hoặc tài liệu.

```bash
read -r -s -p 'New IoT device connection string: ' IOTHUB_DEVICE_CONNECTION_STRING
echo
export IOTHUB_DEVICE_CONNECTION_STRING

# Smoke test 1.000 dòng (tạo checkpoint riêng cho lần gửi thật)
python simulation_iot_full_100m.py \
  --limit 1000 --rate 30 --machines 5 \
  --checkpoint chk_live.json --summary sum_live.json

# Tiếp tục cùng phiên chạy, không giới hạn --limit => đến hết file
python simulation_iot_full_100m.py \
  --resume --rate 30 --machines 5 \
  --checkpoint chk_live.json --summary sum_live.json
```

**Lưu ý:** Có một registered device/client thì chỉ có một sender tuần tự để tránh các vấn đề dùng đồng thời một SDK client. `--rate` là tốc độ **tối đa** được lên lịch, không bảo đảm đạt được tốc độ đó.

## 4. Nhiều kết nối để thử throughput cao hơn

Nếu cần thông lượng lớn, đăng ký **nhiều DeviceId khác nhau trong cùng một IoT Hub**, mỗi DeviceId có connection string riêng. Một file `devices.txt` không được đưa lên Git:

```text
HostName=YOUR_HUB.azure-devices.net;DeviceId=sim-device-01;SharedAccessKey=YOUR_NEW_KEY_01
HostName=YOUR_HUB.azure-devices.net;DeviceId=sim-device-02;SharedAccessKey=YOUR_NEW_KEY_02
HostName=YOUR_HUB.azure-devices.net;DeviceId=sim-device-03;SharedAccessKey=YOUR_NEW_KEY_03
```

Ví dụ trên là placeholder; thay bằng khóa hợp lệ đã cấp cho từng thiết bị. Sau khi tự tạo file này trên VM:

```bash
chmod 600 devices.txt
python simulation_iot_full_100m.py \
  --connections-file devices.txt \
  --rate 500 --machines 5 \
  --checkpoint chk_parallel.json --summary sum_parallel.json
```

`--rate 500` là trần tổng **500 send attempts/giây**, bao gồm retry, không phải 500/s cho từng worker. Chỉ nâng tốc độ sau khi quan sát Azure Monitor và đảm bảo quota/cấu hình ASA và Blob đáp ứng.

## 5. Tham số hữu ích

| Tham số | Mặc định | Công dụng |
|---|---:|---|
| `--source` | URL Parquet Hugging Face | Đường dẫn local hoặc `hf://` |
| `--machines` | 5 | Số máy *logic* thể hiện trong payload, **khác** số registered device |
| `--batch-size` | 4096 | Mỗi lần đọc một lô Parquet |
| `--queue-size` | 2000 | Tối đa 2.000 bản ghi đợi trong bộ nhớ |
| `--rate` | 50 | Trần tổng send attempts/giây, cần >0 khi gửi thật |
| `--limit` | không giới hạn | Chạy thử đến source row index này (exclusive) |
| `--retries` | 5 | Số lượt thử tối đa trên một message |
| `--checkpoint-every` | 1000 | Ghi checkpoint sau từng ngưỡng 1.000 dòng xác nhận liên tiếp |
| `--checkpoint-seconds` | 15 | Ghi checkpoint định kỳ theo thời gian |
| `--resume` | false | Nối tiếp checkpoint cũ, có thể gửi lặp một số dòng |
| `--dry-run` | false | Không gửi IoT Hub |

## 6. Telemetry JSON và SQL

Mỗi JSON chứa các trường **giữ nguyên tên** SQL đang xử lý: `MachineId`, `ProductionLine`, `SensorType`, `SensorValue`, `unit`, `SourceAnomalyFlag`, `EventTimestamp`. Các trường bổ sung `ReplayRunId`, `ReplayRowIndex`, `SourceReadingId`, `SourceMachineId`, `SourceProductionLine` giữ provenance để phân biệt bản ghi nguồn.

Nếu giá trị cảm biến không chuyển được sang số hữu hạn thì gửi JSON `null`; ASA sẽ lọc các bản ghi này. `SourceAnomalyFlag` null được giữ là null trong payload nhưng SQL hiện tại biến thành 0; không nên diễn giải 0 đó là nhãn gốc chắc chắn bình thường.

Nếu một dòng được SDK xác nhận nhưng checkpoint chưa lưu và tiến trình bị gián đoạn, lần `--resume` **có thể gửi lại cùng dòng**. Truy vấn ASA hiện tại **không** deduplicate theo `ReplayRunId + ReplayRowIndex`. Do đó phải ghi rõ *at-least-once* khi lập báo cáo; nếu cần exactly-once cần thiết kế cơ chế idempotency/dedup riêng ở downstream.

## 7. File đo lường

- `iot_100m_checkpoint.json`: vị trí dòng nguồn **liên tiếp đã được xác nhận**, `run_id`, số send thành công, send failed attempts, bytes, histogram độ trễ, stats theo cảm biến/máy.
- `iot_100m_summary.json`: trạng thái (`stopped_at_limit` hoặc `complete_source`), `next_row_acknowledged`, tổng số bytes telemetry gửi, thời gian, thông lượng trung bình, latency mean và p50/p95 **xấp xỉ**.
- Azure Monitor: đối chiếu IoT Hub telemetry metrics; ASA Input Events/Output Events, backlog, Watermark Delay, SU Memory Utilization; Blob transactions/ingress.

SDK send latency **không phải end-to-end latency** VM → Blob → Power BI. Khi dùng `--resume`, tổng số *lần* SDK gửi thành công có thể vượt quá số dòng duy nhất. `payload_bytes_total` là tổng byte JSON đã được SDK xác nhận (không phải kích thước Parquet hoặc Blob và chưa tính protocol/network overhead).

## 8. Lưu ý trước khi chạy 100 triệu messages

- Kiểm tra quota theo gói IoT Hub: Basic B3 hoặc Standard S3 **1 unit** có hạn mức **300 triệu đơn vị message/ngày**; một message có thể tốn hơn một đơn vị nếu lớn hơn 4 KiB. Throttle tối đa của B3/S3 là khoảng **6.000 D2C send operations/giây/unit** ở mức Hub, không phải thông lượng được bảo đảm cho Python/ASA.
- 100 triệu dòng với 500 attempts/s lý tưởng vẫn cần ~55,6 giờ để gửi hết; với 1.200/s ~23,1 giờ, giả sử không có lỗi/thời gian chờ. Tính tổng chi phí IoT Hub, ASA, VM và Storage theo giá Azure hiện hành.
- `SlidingWindow(minute, 3)` có thể tạo nhiều kết quả và lặp cảnh báo ở tải lớn; bảo đảm output/blob/Power BI không bị quá tải.
- Nếu ngừng bằng Ctrl+C hoặc VM bị dừng, chạy lại bằng `--resume` cùng checkpoint và source. **Không xoá checkpoint** nếu cần tiếp tục cùng một lượt thử.
- Thêm `iot_100m_checkpoint.json`, `iot_100m_summary.json`, `devices.txt` vào `.gitignore` nếu không muốn commit log/credentials. Tránh ghi một connection string thật trong file nguồn.

## 9. Kiểm thử offline

```bash
python -m unittest -v test_simulation_iot_full_100m.py
```

Các test dùng Parquet reader và SDK **giả lập**, kiểm thử payload, giới hạn, checkpoint, lỗi gửi, và resume. Chúng **không** chứng minh hệ thống đã chạy thành công với 100 triệu dòng trên Azure.
