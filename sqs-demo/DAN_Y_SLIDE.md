# Dàn ý slide — Amazon SQS và QueueLab

Bạn tự thiết kế slide theo dàn ý dưới đây. Thời lượng gợi ý 10–12 phút, gồm khoảng 4 phút demo. Trình bày lý thuyết trước, chuyển sang sản phẩm từ slide 7.

| Slide | Nội dung trên màn hình | Gợi ý diễn giải | Hình nên dùng |
|---|---|---|---|
| 1. Tên đề tài | Amazon SQS — xử lý đơn hàng bất đồng bộ; sản phẩm QueueLab; tên nhóm/lớp | Nêu bài toán giờ cao điểm: đơn vào nhanh hơn tốc độ xử lý. | Giao diện QueueLab |
| 2. SQS là gì? | Dịch vụ hàng đợi thông điệp do AWS quản lý; tách producer và consumer | Producer gửi việc vào queue, consumer chủ động nhận để xử lý sau. SQS không tự chạy mã nghiệp vụ. | Sơ đồ Producer → SQS → Consumer |
| 3. Chức năng và thành phần | Send, Receive, Delete; queue; message; receipt handle; long polling | Nhận chưa xóa; receipt handle thuộc một lần nhận, khác Message ID. | Sơ đồ vòng đời message |
| 4. Standard và FIFO | Standard: at-least-once, best-effort ordering; FIFO: thứ tự trong message group, deduplication | QueueLab chọn Standard để minh họa retry và xử lý trùng. FIFO không thay thế kiểm soát tác động nghiệp vụ. | Bảng 2 cột ngắn |
| 5. Timeout, retry, DLQ | Receive → ẩn tạm → xóa hoặc hết timeout → nhận lại; ngưỡng demo 3 | Visibility 10 giây chỉ là cấu hình bài demo; không phải mặc định AWS. DLQ giúp giữ message lỗi để phân tích. | Timeline và nhánh DLQ |
| 6. Ưu, nhược, bảo mật, chi phí | Ưu: giảm phụ thuộc, hấp thụ tải, quản lý sẵn; nhược: trùng, độ trễ, quan sát phức tạp; IAM tối thiểu, không public; tính phí request | Giải thích đánh đổi của xử lý bất đồng bộ. Giá hiện hành xem AWS; dừng app sau demo để ngừng thăm dò. | 4 nhóm từ khóa |
| 7. Sản phẩm QueueLab | Producer, worker, SQLite effects, DLQ; backend Local/AWS | Chỉ rõ bản đang mở là mô phỏng cục bộ. Mã có adapter Boto3 nhưng chưa chạy tích hợp thật vì CloudShell chờ xác minh tài khoản. | Sơ đồ kiến trúc + nhãn backend |
| 8. Demo trực tiếp | 3 đơn mẫu → worker → retry → DLQ → gửi lại cùng ID | Làm theo kịch bản bên dưới. Tập trung nhật ký, số lần giao và số kết quả nghiệp vụ. | Mở app thay slide |
| 9. Bằng chứng AWS thật | Region Sydney; Standard queue + DLQ; cùng Message ID nhận 1, 2, 3 và đến DLQ | Console nhận thủ công, không xóa để mô phỏng xử lý chưa hoàn tất. JSON behavior không tự thực thi trong Console. Không nói app đã kết nối thật. | Ảnh AWS trong báo cáo |
| 10. Kết quả và kết luận | 32 tests đạt; local: 2 kết quả, 1 DLQ; duplicate không tăng effect; giới hạn và hướng mở rộng | Phân biệt 20 model/business + 6 fake SDK + 6 HTTP với kiểm chứng Console. Hướng mở rộng: DB chia sẻ, nhiều worker, giám sát, retry/backoff. | Bảng kết quả + link GitHub |

## Kịch bản demo khoảng 4 phút

- 0:00–0:35: mở trang giới thiệu, nêu mục đích và nhãn **Mô phỏng cục bộ**. Chuyển vào phòng thực hành.
- 0:35–1:05: worker dừng, tạo 3 đơn mẫu. Chỉ ra producer vẫn nhận đơn khi consumer chưa chạy.
- 1:05–2:00: bật chạy tự động. Trong khi chờ 30–40 giây, giải thích visibility timeout và retry; quan sát đơn lỗi tạm thời được nhận lần 2.
- 2:00–2:35: tạm dừng, kiểm tra DLQ. Kết quả dự kiến: normal và fail_once hoàn tất; always_fail ở DLQ; effect tổng bằng 2.
- 2:35–3:15: gửi lại đơn đã hoàn tất với cùng ID, nhận một lần. Số lần giao tăng, effect đơn vẫn là 1; nhật ký ghi bỏ qua trùng.
- 3:15–4:00: mở ảnh bằng chứng AWS: cùng Message ID qua ba lần nhận rồi xuất hiện trong DLQ. Nhắc phạm vi kiểm chứng.

## Câu hỏi có thể được hỏi

**SQS có tự xử lý đơn không?** Không. Ứng dụng consumer thực hiện nghiệp vụ; SQS quản lý chuyển phát và lưu message.

**Vì sao nhận rồi vẫn còn message?** ReceiveMessage chỉ ẩn message trong visibility timeout. Consumer thành công cần DeleteMessage với receipt handle mới nhất.

**Tại sao phải chống trùng?** Standard có thể chuyển phát nhiều lần; lỗi sau khi ghi kết quả nhưng trước lúc xóa cũng gây nhận lại. QueueLab có khóa order_id trong sổ kết quả để minh họa idempotency.

**FIFO có đảm bảo mọi tác động chỉ xảy ra một lần không?** Không thể suy ra điều đó cho toàn hệ thống; vẫn phải phối hợp ghi dữ liệu, retry và side effect bên ngoài. Deduplication của FIFO có phạm vi và cửa sổ thời gian.

**Tại sao dùng mô phỏng?** Để trình diễn ổn định, quan sát nhanh và không cần credentials. Đã kiểm chứng riêng Send/Receive/redrive trên SQS thật. Không đánh đồng hai môi trường.

**Đã test AWS SDK thật chưa?** Chưa. CloudShell báo tài khoản đang xác minh. Sáu test adapter dùng fake client; repo có hướng dẫn chạy SDK/CLI khi có phiên AWS hoạt động.

**Có miễn phí hoàn toàn không?** Không cam kết. SQS có mức miễn phí công bố trên trang giá, nhưng tổng hóa đơn phụ thuộc request, truyền dữ liệu, dịch vụ khác và điều kiện tài khoản.

## Tài nguyên cho slide

- Repo: https://github.com/st4rlight3/aws-sqs-queuelab-demo
- Báo cáo: `Bao_cao_Amazon_SQS_QueueLab.docx` — lý thuyết trước, demo sau.
- Ảnh: `Giao_dien_QueueLab.jpg`, `Bang_chung_AWS_SQS.jpg` trong bộ bàn giao.
- Nguồn chính: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html và các liên kết cuối báo cáo.

Đây là dàn ý cho bài SQS; liên kết Canva của bài EC2 trước đó không được dùng làm slide của bài mới.
