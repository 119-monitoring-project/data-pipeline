## data-pipeline
이 레포지토리는 응급의료기관의 데이터 파이프라인을 구축한 코드와 설정 파일을 포함하고 있습니다. <br>
이 데이터 파이프라인은 데이터 수집, 전처리를 자동화하는 데 사용됩니다. <br>
프로젝트의 자세한 설명은 [project.readme](https://github.com/119-monitoring-project)를 참고해주세요:)

### 💡 summary
| Directory | Explanation |
|:---:|:---|
| `airflow/dags` | airflow dag 관련 파일 |
| `airflow/module/util` | dag를 실행시키기 위한 util 모듈 |
| `airflow/test`  | airflow test 관련 파일 |
| `kafka` | 실시간 수집을위한 kafka 관련 파일 |

### 📋 execution
1. github repository 복사하기
```bash
https://github.com/119-monitoring-project/data-pipeline.git
```

2. docker compose 실행하기
```bash
docke-compose -f airflow-docker-compose.yaml up --build
```
