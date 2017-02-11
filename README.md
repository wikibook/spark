# 빅데이터 분석을 위한 스파크 2 프로그래밍 
빅데이터 분석을 위한 스파크 2 프로그래밍 서적의 소스코드입니다.
아래는 소스코드 전반에 대한 설명 및 유의사항, 오류 정정등과 관련된 내용으로 사용하기 전에 반드시 먼저 살펴보시기 바랍니다.

## 로컬 개발 환경 
* 본 프로젝트는 자바7 및 자바8 예제가 동일 소스에 포함되어 있으므로 자바8 설치가 필요합니다.
* 본 도서는 본문에서 Scala IDE를 다루고 있으나 IntelliJ등 어떤 IDE를 사용하여도 무방합니다.
   * 단, 사용하는 IDE에 따라 실제 오류가 아닌 코드를 오류로 인식하는 문제가 발생할 수 있으므로
   * maven 빌드를 수행하여 실제 컴파일 오류인지 IDE의 버그 또는 개발 환경 문제로 인한 것인지 여부를 확인해 보아야 합니다.  

## 코드 관련 유의사항     
* 본 도서 본문에 실린 예제 코드 중에서 스파크세션(SparkSession) 생성 시 사용한 "spark.local.ip", "spark.driver.host", "spark.sql.warehouse.dir" 설정은 의미 없거나 매번 지정해야 하는 필수 설정값이 아니므로 제공되는 예제 코드에서는 필요한 경우를 제외하고는 이를 사용하지 않는 것으로 수정하여 배포합니다.
* 데이터프레임등 일부 예제 코드의 경우는 다수의 메서드 사용법에 대한 예제를 하나의 코드에 포함하고 있으므로 각 예제를 메서드로 구분하고 아래와 같이  메인함수 내에 주석으로 처리하여 제공합니다. 따라서 실제 코드 테스트 시에는 원하는 부분의 주석을 해제하고 실행해야 합니다.

``` 
// [예제 실행 방법] 아래에서 원하는 예제의 주석을 제거하고 실행!!
// createDataFrame(spark, spark.sparkContext)
// runBasicOpsEx(spark, sc, sampleDf)
// runColumnEx(spark, sc, sampleDf)
```
* 개인 로컬 환경에서 예제 실행 시 `Can't assign requested address: Service 'sparkDriver' failed...` 오류가 반복하여 발생될 경우 스파크 컨텍스트 또는 스파크 세션 생성시 `config("spark.driver.host", "127.0.0.1")`와 같은 형태로 드라이버의 호스트명 또는 IP를 명시해 줍니다.
* 일부 코드의 경우 코드 상에 본문의 "절 번호"를 포함하였으나 쉽게 판별이 가능한 경우 기록하지 않았습니다. 
* 자바, 스칼라, 파이썬 코드는 거의 대부분 동일한 이름의 파일명, 메서드명을 사용하는 것을 원칙으로 하였으나 일부 언어에 따른 특성 상 일치하지 않는 경우도 있습니다. 
* 인텔리제이 사용 시 Project Settings -> Modules -> Dependences 탭에서 스칼라(scala-sdk-2.11.*)와 파이썬 라이브러리 및 pyspark 라이브러리(`<spark_home>/python/pyspark.zip, <spark_home>/python/lib/py4j-0.10.4-src.zip`)를 추가해 주어야 합니다.  
* 스칼라IDE는 가급적 최신 버전 사용을 권장합니다. 
* 스칼라IDE 구 버전의 경우 자바 코드 내에서 스칼라 오브젝트를 인식하지 못하는 경우가 종종 발생되는데 이 경우 Project -> Properties -> Java Build Path -> Order and Export 탭에서 "Scala Library container" 항목을 토글(toggle, 선택과 해제를 반복)하는 방식으로 재 빌드를 유도하면 해결되는 경우가 있습니다.

## 오류 사항
* 일부 예제 코드에서 스파크 컨텍스트 또는 스파크 세션 생성 시 사용한 "spark.local.ip"는 사용되지 않는 속성값이므로 실제 코드에서는 사용할 필요가 없습니다.

* 266 페이지   
  df.show ---> df4.show 

* 295 페이지, 5.5.2.4.17절   
  df.show ---> personDF.show

* 429 페이지 예제 8-11(pipeline_sample.py)  
   training.show(False) --> training.show(truncate=False)  
   assembled_training.show(False) --> assembled_training.show(truncate=False)  
   LogisticRegression(... weightCol="gender") --> LogisticRegression(... labelCol="gender")   




