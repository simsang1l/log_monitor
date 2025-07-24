# Git 설정
1. 원격 Repository 설정
    ```bash
    # 키가 없으면
    ssh-keygen -t ed25519 -C "you@example.com"
    cat ~/.ssh/id_ed25519.pub   # ➜ GitHub Settings ▸ SSH Keys
    ```

    ```bash
    # 원격 Repository 설정
    git remote add origin git@github.com:your-username/your-repository.git
    cd ssh-analytics-pipeline
    ```

2. local 환경 설정
    ```bash
    git config --global user.name "your-name"
    git config --global user.email "your-email@example.com"
    ```

# Logstash 설정
1. Logstash 설정 파일 위치 지정
    ```bash
    cd pipeline/
    vi logstash.conf # 파일 수정
    ```

2. 설정 파일 세팅
    logstash는 `input`, `filter`, `output` 세 단계로 구성되어 있음

    ```bash
    input {
        file {
            path => "/data/SSH.log"
        }
    }
    ```
    - `path` : 로그 파일 경로
    - `start_position` : 로그 파일 시작 위치
    - `sincedb_path` : 읽은 위치 저장
    - `mode` : 파일 모드
    - `ignore_older` : 오래된 파일 무시

    ```bash
    filter {
        grok {
            match => { "message" => "%{SYSLOGTIMESTAMP:logdate} %{HOSTNAME:host} %{DATA:process}(?:\[%{NUMBER:pid}\])?: %{GREEDYDATA:msg}" }
        }
    }
    ```
    - `grok` : 로그 파일 형식 파싱
    - `match` : 로그 파일 형식 파싱

    ```bash
    output {
        stdout { codec => rubydebug }
    }
    ```
    - `stdout` : 터미널에 출력

3. 설정 파일 적용
    ```bash
    docker compose up -d

    # 혹시 띄워져 있는 상태에서 수정했다면
    docker compose down
    docker compose up -d
    ```

4. 결과 확인   
    ```bash
    docker logs -f logstash
    # 또는
    docker logs --tail 100 logstash
    ```

# Kafka 설정
1. Kafka 토픽 생성
    ```bash
    docker exec -it kafka1 kafka-topics --create --topic ssh-log --bootstrap-server kafka1:29092 --partitions 3 --replication-factor 3
    ```

2. Kafka 토픽 확인
    ```bash
    docker exec -it kafka1 kafka-topics --list --bootstrap-server kafka1:29092
    ```

    ```bash
    # 토픽 삭제
    docker exec -it kafka1 kafka-topics --delete --topic ssh-log --bootstrap-server kafka1:29092
    ```

3. 토픽 메시지 개수 확인
    - 터미널 접속
        ```bash
        docker exec -it kafka kafka-console-consumer --topic ssh-log --bootstrap-server kafka:29092 --from-beginning --max-messages 10
        ```

    - kafka-ui 접속
        ```bash
        http://localhost:8080
        ```

# elasticsearch 설정
1. elasticsearch 인덱스 삭제
    ```bash
    curl -X DELETE "http://localhost:9200/ssh-log"
    ```

2. elasticsearch 인덱스 확인
    ```bash
    curl -X GET "http://localhost:9200/_cat/indices"
    ```