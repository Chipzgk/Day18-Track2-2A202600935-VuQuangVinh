# Lab 18 Reflection — Vũ Quang Vinh

## Anti-pattern dễ vướng nhất: Writing directly to Gold

Trong thực tế nhóm em dễ bỏ qua tầng Silver và
viết thẳng dữ liệu raw vào Gold để "tiết kiệm bước".

Lab này cho thấy hậu quả rõ ràng: Bronze có 200,000
rows nhưng chứa 9,948 duplicates. Nếu aggregate thẳng
lên Gold mà không qua Silver dedup, các metric p50/p95
latency và cost_usd sẽ bị sai lệch do đếm trùng request.

Tầng Silver không chỉ là bước trung gian, mà nó là nơi
enforce data quality trước khi business logic chạy ở Gold.
Bỏ Silver đồng nghĩa với việc đưa garbage vào các
dashboard mà người dùng cuối tin tưởng.

Bài học: Medallion architecture tốn thêm một bước write
nhưng đổi lại là lineage rõ ràng và khả năng debug khi
số liệu Gold bất ngờ sai — chỉ cần kiểm tra Silver là
tìm ra nguồn gốc vấn đề ngay.