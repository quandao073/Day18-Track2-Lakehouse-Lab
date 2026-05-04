# Reflection — Day 18 Lakehouse Lab

**Họ tên:** Đào anh Quân
**Mã học viên:** 2A202600028
**Anti-pattern nguy hiểm nhất với tôi: Small-file problem**

Trong pipeline LLM observability (NB4), Bronze layer nhận log từ nhiều model
qua streaming ingestion — mỗi batch nhỏ từ vài chục đến vài trăm request.
Nếu mỗi micro-batch ghi một Parquet file riêng, sau 24 giờ Bronze sẽ tích lũy
hàng nghìn file nhỏ. Query cost tăng gấp nhiều lần vì Spark/DuckDB phải mở
từng file để đọc footer metadata, dù chỉ cần lọc theo `date` hoặc `model`.

NB2 đã minh họa điều này rõ ràng: 200 append-batch tạo ra 200 file, truy vấn
chạy chậm hơn nhiều so với sau khi `OPTIMIZE + ZORDER`. Trong production,
streaming pipeline chạy liên tục 24/7 mà không có compaction schedule thì đây
là nợ kỹ thuật tích lũy âm thầm — không báo lỗi, chỉ chậm dần.

Cách phòng ngừa: đặt lịch `OPTIMIZE ZORDER BY (model, date)` chạy mỗi 1–6
giờ trên Silver và Gold; giới hạn batch size tối thiểu ở Bronze ingestion;
và monitor `numFiles` qua `DESCRIBE DETAIL` như một health metric.

Anti-pattern này đặc biệt dễ bị bỏ qua vì giai đoạn đầu data ít, query vẫn
nhanh — chỉ lộ ra khi traffic thật đến.
