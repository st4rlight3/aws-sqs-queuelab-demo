# QueueLab — Demo Amazon SQS

Sản phẩm học tập minh họa hàng đợi xử lý đơn hàng: gửi/nhận, visibility timeout, thử lại, DLQ và xử lý trùng. Giao diện có phần giải thích dịch vụ trước khi mở phòng thực hành.

## Chạy nhanh trên máy cá nhân

Cần Python 3.10 trở lên. Bản cục bộ chỉ dùng thư viện chuẩn, không cần tài khoản AWS.

```powershell
cd sqs-demo
python app.py --open-browser
```

Trên Windows có thể bấm `START_DEMO.cmd`. Mở http://127.0.0.1:8081/ nếu trình duyệt chưa tự mở. Dừng bằng Ctrl+C trong terminal. Dữ liệu được lưu ở `data/local/`; để chạy buổi mới, dùng `python app.py --data-dir data/buoi-moi --open-browser`.

**Bản mặc định là mô hình cục bộ, không kết nối Amazon SQS.** Nhãn backend luôn xuất hiện trên giao diện. SQLite giữ trạng thái queue và sổ ghi nhận nghiệp vụ.

## Kịch bản trình diễn 4–5 phút

1. Giữ worker dừng, bấm **Tạo 3 đơn mẫu**: 3 đơn chờ xử lý, chưa có kết quả nghiệp vụ.
2. Bấm **Chạy tự động**. Đơn bình thường hoàn tất; đơn lỗi tạm thời được nhận lại sau timeout rồi hoàn tất; đơn lỗi liên tục được chuyển sang DLQ sau ngưỡng 3 lần nhận.
3. Chờ khoảng 40 giây, bấm **Tạm dừng** rồi **Kiểm tra DLQ**. Kỳ vọng trong bản cục bộ: 2 kết quả nghiệp vụ và 1 message ở DLQ.
4. Bấm **Gửi lại cùng ID** trên đơn hoàn tất, rồi nhận một message. Số lần giao tăng nhưng số kết quả của đơn đó vẫn là 1; nhật ký ghi bỏ qua trùng.
5. Tùy chọn: nhận một đơn mới rồi bấm **Giả lập worker chết** trong 4 giây xử lý; không có DeleteMessage nên message xuất hiện lại sau timeout.

Nhận message không đồng nghĩa xóa message. Worker ghi nhận kết quả bền vững trước khi xóa bằng receipt handle. Nếu xóa lỗi, lần nhận lại dựa vào order_id để bỏ qua tác động nghiệp vụ lặp.

## Những gì đã được kiểm chứng

- **32/32 kiểm thử đạt:** 20 ca mô hình/nghiệp vụ, 6 ca adapter AWS dùng fake client và 6 ca HTTP. Chi tiết: `tests/verification.json`.
- **AWS Console thật, ngày 28/10/2026:** tạo 2 Standard queue ở `ap-southeast-2`; gửi 1 message; quan sát cùng Message ID được nhận lần 1, 2, 3; sau đó xuất hiện tại DLQ. Chi tiết đã lược bỏ account ID: `evidence/aws-console-verification.json`.
- Chưa thực thi ứng dụng/SDK với tài khoản AWS. CloudShell của tài khoản đang chờ xác minh. Bộ kiểm thử adapter dùng fake client không phải kiểm thử tích hợp AWS.
- Console không chạy mã nghiệp vụ trong JSON. Các lần nhận thủ công không xóa chỉ minh họa tình huống xử lý chưa hoàn tất.

## Chạy với Amazon SQS thật

Cần phiên AWS đã xác thực và quyền trên hai queue. Không đặt access key vào mã, file cấu hình, README hoặc GitHub. Dùng IAM role/phiên tạm thời hoặc profile được quản trị sẵn. `AWSQueue` dùng chuỗi cung cấp thông tin xác thực mặc định của Boto3.

```powershell
python -m pip install -r requirements-aws.txt
python aws/write_config.py --region ap-southeast-2 --queue-url YOUR_MAIN_QUEUE_URL --dlq-url YOUR_DLQ_URL
python app.py --backend aws --config .queuelab_aws.json --open-browser
```

Thay hai URL bằng queue của bạn, cùng Region. File `.queuelab_aws.json` chứa Region và URL, không chứa thông tin xác thực; đã được bỏ qua bởi Git.

Nếu muốn tạo một cặp queue riêng bằng SDK (có thể phát sinh request AWS):

```powershell
python aws/setup.py --region ap-southeast-2
python aws/setup.py --region ap-southeast-2 --create
```

Lệnh đầu chỉ in kế hoạch. Lệnh `--create` tạo hai queue tên duy nhất, SSE-SQS, source retention 1 ngày, DLQ 2 ngày, visibility 10 giây, long polling 2 giây, maxReceiveCount 3; giới hạn redrive của DLQ về source queue. Không sửa queue đã tồn tại. Quyền tạo/cấu hình cần được cấp riêng, khác với runtime policy mẫu.

Trong AWS CloudShell đã hoạt động, có thể clone repo riêng sau khi xác thực GitHub, cài Boto3 nếu cần, tạo cấu hình rồi chạy bản CLI:

```bash
cd sqs-demo
python3 aws/write_config.py --region ap-southeast-2 --queue-url YOUR_MAIN_QUEUE_URL --dlq-url YOUR_DLQ_URL
python3 aws/run_scenarios.py
python3 aws/run_scenarios.py --run
```

Lệnh không có `--run` chỉ in kế hoạch. Lệnh có `--run` gửi 3 đơn giả, chạy cùng worker nghiệp vụ và in JSON kết quả; giữ message lỗi trong DLQ. Đây là hướng dẫn chạy lại, chưa phải kết quả đã xác nhận với AWS của bản bàn giao. Giao diện web nên chạy trên máy cá nhân có phiên AWS, không phụ thuộc web preview của CloudShell.

`aws/iam-runtime-policy.example.json` là ví dụ quyền runtime, cần thay placeholder ARN. Không có chính sách nào tự động được gắn vào tài khoản.

## Kiến trúc và giới hạn

```
Trình duyệt → HTTP API Python → Producer → Queue → Worker → SQLite effects
                                                 ↓ lỗi nhiều lần
                                                DLQ
```

Backend có thể là LocalQueue (SQLite) hoặc AWSQueue (Boto3). Queue thật do Amazon SQS quản lý; sổ kết quả vẫn ở SQLite của ứng dụng. Khóa duy nhất order_id minh họa idempotency trong phạm vi một sổ kết quả bền vững. Không bảo đảm exactly-once cho mọi tác động bên ngoài như thanh toán/email.

Mô hình cục bộ không tái tạo toàn bộ SQS: số đếm chính xác, không mô phỏng hạ tầng phân tán/giới hạn throughput/ordering; receipt cũ bị từ chối chặt hơn AWS; receive count được đặt lại khi chuyển DLQ. SQS thật có số đếm gần đúng và đặc tính at-least-once của Standard queue.

Bản học tập chỉ lắng nghe loopback. Chưa có xác thực đa người dùng, HTTPS đầu cuối, cảnh báo CloudWatch, cơ chế lease/idempotency phân tán hay cơ sở dữ liệu chia sẻ cho nhiều worker. Không mở trực tiếp ra Internet.

## Kiểm thử

```powershell
python tests/test_demo.py
```

Tests dùng cơ sở dữ liệu tạm, server loopback cổng ngẫu nhiên và fake AWS client; không tạo tài nguyên hay gọi AWS thật.

## Chi phí và kết thúc demo

SQS tính phí theo lượng sử dụng; AWS công bố 1 triệu request đầu mỗi tháng miễn phí, nhưng ưu đãi và tài khoản cần được kiểm tra trên trang giá hiện hành. Không coi mọi tài nguyên AWS là miễn phí. Bản web AWS thăm dò số đếm định kỳ, do đó dừng app sau buổi học. Không dùng polling liên tục khi không cần.

Hai queue Console `queuelab-20261028-demo2-orders` và `queuelab-20261028-demo2-dlq` ở Sydney còn được giữ để trình diễn lại. Queue chứa dữ liệu giả; bộ bàn giao không tự xóa/purge tài nguyên. Sau khi lưu bằng chứng và không cần dùng nữa, vào đúng Region, kiểm tra tên rồi tự xóa hai queue trong Console. Xóa queue/message không thể phục hồi theo luồng thông thường.

## Tài liệu AWS

- [Giới thiệu SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [Standard queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html)
- [Visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Dead-letter queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Giá dịch vụ](https://aws.amazon.com/sqs/pricing/)
