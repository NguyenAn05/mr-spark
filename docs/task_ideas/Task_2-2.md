# Bài 2-2 - Spark DataFrame: P80/P90 động theo SKU-tháng

> Mục đích của tài liệu này là giải thích đề và đưa ra lộ trình triển khai. Đây **không** phải code nộp bài; bạn nên tự hiện thực theo các bước bên dưới.

## 1. Đề bài thực sự yêu cầu gì?

Với **mỗi cặp `(SKU, tháng)`**, cần làm đồng thời hai phép tính: P80 và P90.

1. Đếm số mã khuyến mãi của **mỗi dòng đơn hàng**. Phải tính tất cả mã trong `promotion-ids`, bao gồm mã Amazon. Ô trống có số mã bằng `0`.
2. Trong từng nhóm `(SKU, tháng)`, tính ngưỡng percentile động cho số mã đó: P80 và P90.
3. Với từng mốc, chỉ giữ các đơn có `promotion_count >= threshold`.
4. Trên tập đơn còn lại, tính **population standard deviation** của `Amount`: dùng `stddev_pop`, không dùng `stddev` hay `stddev_samp`.
5. Nếu sau khi lọc có ít hơn 2 đơn, độ lệch chuẩn phải bằng `0.0`.

Đề bắt buộc có cả hai cách tính percentile:

- Spark `percentile_approx` hoặc `approx_percentile`.
- Cách **exact do nhóm tự hiện thực** chỉ bằng DataFrame/Dataset API.

Không dùng `spark.sql("...")` hoặc chuỗi SQL cho lời giải chính.

## 2. Những điều dễ hiểu sai

### 2.1. Percentile là động, không phải một số chung cho cả dữ liệu

Không được tính một P80/P90 cho toàn file rồi áp cho mọi SKU. Mỗi nhóm `(SKU, tháng)` có phân phối số promotion riêng và phải có threshold riêng.

Ví dụ một nhóm có số mã:

```text
0, 0, 1, 1, 2, 2, 2, 3, 5, 9
```

thì P90 có thể là `5` với nearest-rank/approx, nhưng là `5.4` với exact nội suy tuyến tính. Nhóm khác có thể có kết quả khác hoàn toàn.

### 2.2. Đếm mã khuyến mãi theo từng dòng đơn hàng

`promotion-ids` là chuỗi có các mã ngăn bằng dấu phẩy. Cách xử lý an toàn:

```text
null hoặc "" -> [] -> promotion_count = 0
"A,B"        -> ["A", "B"] -> 2
```

Nên `trim` mỗi mã rồi bỏ chuỗi rỗng. Không lọc mã Amazon và không tái sử dụng điều kiện “temporally valid” của Bài 2-1: Bài 2-2 yêu cầu **tất cả** promotion identifiers.

Đề không bảo loại mã lặp trong cùng một ô. Cách bám sát chữ “number of promotion identifiers” là đếm từng mã không rỗng sau khi tách. Nếu bạn dùng `array_distinct`, hãy ghi rõ trong report vì nó thay đổi `promotion_count` khi dữ liệu có mã lặp.

### 2.3. Population standard deviation

Hàm đúng là:

```scala
stddev_pop(col("amount"))
```

Ví dụ hai amount `499` và `599` có trung bình `549`; population standard deviation là `50`, còn sample standard deviation xấp xỉ `70.71`.

`stddev_pop` bỏ qua `Amount` null. Khi group sau lọc có ít hơn hai đơn, hoặc Spark trả null vì không có amount hợp lệ, xuất `0.0` bằng `when` và `coalesce`.

### 2.4. “Exact percentile” phải chốt định nghĩa

Đề không nói công thức exact cụ thể. Một lựa chọn rõ ràng, phổ biến và phù hợp slide tham khảo là **linear interpolation**:

```text
Cho N giá trị đã sort tăng dần: x[0], ..., x[N-1]
h = (N - 1) * p
lo = floor(h), hi = ceil(h)
Pp = x[lo] + (h - lo) * (x[hi] - x[lo])
```

Với ví dụ 10 giá trị trên, P90 có `h = 8.1`, nên nội suy giữa `x[8] = 5` và `x[9] = 9`, được `5.4`.

Trong report phải ghi đúng công thức đã chọn. Nếu chọn nearest-rank thay vì nội suy, cũng cần nêu rõ; tuyệt đối không gọi hàm percentile có sẵn rồi đặt tên là “self-implemented exact”.

## 3. Thiết kế output nên dùng

Đề chỉ yêu cầu một Parquet cho Bài 2-2. Thiết kế dễ kiểm tra nhất là đưa kết quả của cả hai phương pháp vào cùng file `Task_2-2.parquet`:

| Cột | Kiểu gợi ý | Ý nghĩa |
|---|---|---|
| `month` | string | `yyyy-MM` |
| `sku` | string | SKU của nhóm |
| `method` | string | `approx` hoặc `exact` |
| `percentile_level` | string | `P80` hoặc `P90` |
| `percentile` | double | `0.8` hoặc `0.9` |
| `promotion_threshold` | double | ngưỡng của nhóm |
| `qualifying_order_count` | long | số đơn có count >= ngưỡng |
| `amount_stddev_pop` | double | population standard deviation, hoặc `0.0` |

Một `(month, sku)` có bốn dòng: approximate/exact x P80/P90. Giữ threshold và số đơn đủ điều kiện để người chấm audit kết quả.

## 4. Pipeline DataFrame đề xuất

### Bước A - Chuẩn hoá input

Đọc CSV với header và schema rõ ràng, rồi lấy các cột:

```text
index, Date, SKU, Amount, promotion-ids
```

Tạo DataFrame `orders` với:

- `row_id`: ép `index` sang `long`, dùng để phân biệt dòng.
- `order_date`: `to_date(Date, "MM-dd-yy")`.
- `month`: `date_format(order_date, "yyyy-MM")`.
- `sku`: `trim(SKU)`.
- `amount`: ép `Amount` sang `double`.
- `promotion_count`: số mã không rỗng sau `split` và `trim`.

Chỉ loại bản ghi thiếu `row_id`, `month` hoặc `sku`. Không loại `amount` null trước khi tính percentile, vì percentile là của số promotion, không phải Amount.

Kiểm tra nhanh:

```text
orders.count()
orders.select(min("promotion_count"), max("promotion_count"))
orders.groupBy("month", "sku").count()
```

### Bước B - Tạo hai mức percentile thành DataFrame nhỏ

Đừng copy pipeline P80 rồi sửa thành P90. Tạo một DataFrame hai dòng:

```text
P80, 0.8
P90, 0.9
```

Dùng chung DataFrame này trong cả nhánh approximate và exact. Trong Scala có thể dựng bằng `spark.range(1)`, `array`, `struct`, `explode`, `lit`; không cần SQL string.

### Bước C - Nhánh approximate

Gom `orders` theo `(month, sku)` và gọi `percentile_approx` cho hai mức 0.8, 0.9. Nhận mảng hai threshold rồi `explode` thành hai dòng P80/P90.

Pseudo-code:

```scala
approxThresholds = orders
  .groupBy("month", "sku")
  .agg(percentile_approx(
    col("promotion_count"),
    array(lit(0.8), lit(0.9)),
    lit(10000)
  ))
  // đổi mảng threshold thành các dòng P80/P90
  .withColumn("method", lit("approx"))
```

`accuracy` là đánh đổi tốc độ/bộ nhớ và độ chính xác. Chọn một giá trị cố định (ví dụ `10000`) và ghi nó trong report. Không gọi kết quả này là exact, kể cả khi dataset nhỏ làm nó trùng exact.

### Bước D - Nhánh exact tự cài đặt bằng Window

Đây là phần quan trọng nhất. Dùng window partition theo `(month, sku)`, sort tăng dần `promotion_count`, rồi thêm `row_number` và số phần tử nhóm `N`.

```text
Window partition: (month, sku)
Window order: promotion_count ASC, row_id ASC
```

`row_id` ở cuối thứ tự giúp chạy lặp lại ổn định. Khi `promotion_count` bằng nhau, thay đổi thứ tự không đổi giá trị percentile.

Với mỗi mức `p`:

1. Gắn `p` vào từng dòng qua cross join với DataFrame hai percentile.
2. Tính `h = (N - 1) * p`, `lo = floor(h)`, `hi = ceil(h)`.
3. Lấy `x_lo` tại `row_number = lo + 1`; lấy `x_hi` tại `row_number = hi + 1`.
4. Group lại theo `(month, sku, percentile_level, p)` để có đúng một dòng threshold.
5. Tính `x_lo + (h - lo) * (x_hi - x_lo)`.
6. Thêm `method = "exact"`.

Chỉ dùng `row_number`, `count`, `floor`, `ceil`, `when`, `max`, `groupBy`, `join`... Đây đều là DataFrame API. Không gọi `percentile`, `percentile_approx`, `collect` hay Scala collection để né phần exact tự cài đặt.

### Bước E - Dùng threshold để tính standard deviation

Viết một hàm nhận `thresholds` (approx hoặc exact) và trả schema chung:

1. Join `orders` với `thresholds` theo `(month, sku)`.
2. Giữ các dòng `promotion_count >= promotion_threshold`.
3. Group theo `(month, sku, method, percentile_level, percentile, promotion_threshold)`.
4. Tính `count(lit(1))` thành `qualifying_order_count` và `stddev_pop(amount)`.
5. Nếu `qualifying_order_count < 2`, đặt deviation bằng `0.0`; với null còn lại cũng `coalesce` về `0.0`.

Sau đó `unionByName` kết quả approximate và exact, rồi `orderBy(month, sku, method, percentile_level)`.

## 5. Pseudo-code tổng thể

```text
raw
  -> orders(month, sku, amount, promotion_count)
  -> approx_thresholds(month, sku, P80/P90, threshold)
  -> exact_thresholds(month, sku, P80/P90, threshold)

orders JOIN approx_thresholds
  -> filter promotion_count >= threshold
  -> groupBy -> stddev_pop
  -> approx_result

orders JOIN exact_thresholds
  -> filter promotion_count >= threshold
  -> groupBy -> stddev_pop
  -> exact_result

approx_result UNION BY NAME exact_result
  -> coalesce(1)
  -> ghi một file Task_2-2.parquet
```

## 6. Kiểm tra tính đúng

Nên đặt các kiểm tra sau trong lúc phát triển:

1. Không có `promotion_count < 0`.
2. Mỗi `method` có đúng hai dòng (P80/P90) trên mỗi `(month, sku)`.
3. Không có `amount_stddev_pop` null hoặc âm.
4. `qualifying_order_count` phải dương, vì threshold được lấy từ chính nhóm.
5. Nhóm chỉ có một order phải có deviation `0.0` ở cả P80 và P90.
6. Chọn một nhóm nhỏ, in `promotion_count` đã sort rồi tính tay để đối chiếu threshold.
7. Đọc lại Parquet bằng Spark local mode; kiểm tra schema, số dòng và vài dòng đầu. Không dùng `getmerge` cho Parquet.

## 7. Phần report bắt buộc

### 7.1. So sánh exact và approximate

Join hai bảng result theo `(month, sku, percentile_level)`, rồi report:

- số nhóm có `exact_threshold != approx_threshold`;
- sai khác tuyệt đối lớn nhất và trung bình của threshold;
- số nhóm có tập qualifying khác nhau (tốt nhất so tập `row_id`, tối thiểu so `qualifying_order_count`);
- số nhóm có `amount_stddev_pop` khác nhau;
- vài ví dụ khác biệt dễ hiểu.

Threshold khác không tự động có nghĩa đáp số cuối khác: `promotion_count` là số nguyên và thường có nhiều giá trị trùng.

### 7.2. Benchmark

Đề yêu cầu ít nhất 5 lần chạy và report phải có mean cùng standard deviation thời gian.

1. Dùng cùng input, Spark config và số partition cho cả hai cách.
2. Đo action thật (`count`, ghi tạm, hoặc action tương đương), không đo chuỗi transformation lazy.
3. Có thể warm-up một lần riêng, sau đó mới đo 5 lần.
4. Ghi từng thời gian, mean, standard deviation.
5. Nếu chạy cả hai cách trong một job, phải có action tách để thời gian không lẫn nhau.

### 7.3. Câu hỏi repartition

Đo nhóm lớn nhất trước:

```text
orders.groupBy(month, sku).count().agg(max(count))
```

Chỉ khi có nhóm lớn hơn 1.000 orders mới phải thảo luận cụ thể về manual repartition. Nếu không có, report vẫn phải ghi số đo và giải thích vì sao không repartition thủ công. So sánh dung lượng nhóm lớn nhất với partition mặc định khoảng 128 MB; không thêm `repartition` chỉ vì “có vẻ nhanh hơn”.

## 8. File nên tạo khi tự code

```text
src/Task_2-2/
  Task22Config.scala
  Task22Query.scala
  Task22App.scala
  SingleParquetExporter.scala  # có thể tái sử dụng helper của 2-1

outputs/
  Task_2-2.parquet
```

Thứ tự làm hợp lý: chuẩn hoá input -> approximate -> kiểm nhóm nhỏ -> exact -> so sánh -> xuất Parquet -> benchmark -> viết report.

### Lệnh chạy sau khi build

Entry point đã được cài đặt là task22.Task22App. Tham số thứ ba là số lượt benchmark và không được nhỏ hơn 5.

~~~bash
spark-submit \
  --master 'local[*]' \
  --conf 'spark.hadoop.fs.defaultFS=file:///' \
  --class task22.Task22App \
  "$JAR_PATH" \
  "./data/Amazon Sale Report.csv" \
  "./outputs/Task_2-2.parquet" \
  5 2>&1 | tee "./outputs/task22-run.log"
~~~

Đường dẫn output phải chưa tồn tại. Log sẽ chứa năm mẫu benchmark cho mỗi phương pháp, mean, population standard deviation, kích thước nhóm SKU-tháng lớn nhất và explain(true).

## 9. Checklist trước khi nộp

- [ ] Scala + Spark DataFrame/Dataset API, không SQL string.
- [ ] Có cả P80 và P90.
- [ ] Có `percentile_approx` và exact percentile tự cài đặt.
- [ ] Exact percentile có công thức ghi rõ trong report.
- [ ] Dùng `stddev_pop`; group < 2 đơn cho `0.0`.
- [ ] Có phân tích khác biệt, benchmark >= 5 run, mean và standard deviation.
- [ ] Có kết luận về nhóm > 1.000 order và repartition.
- [ ] Xuất đúng một file `Task_2-2.parquet`, đọc lại được bằng Spark/Pandas.
- [ ] Bổ sung Bài 2-2 vào `Report.pdf` và cấu trúc ZIP cuối cùng.
