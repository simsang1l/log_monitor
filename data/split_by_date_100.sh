#!/bin/bash

LOG_FILE="data/SSH.log"
OUTPUT_DIR="data"
YEAR="2024"  # 원하는 연도로 수정
MAX_ROWS_PER_FILE=100  # 각 파일당 최대 행 수

mkdir -p "$OUTPUT_DIR"

# 각 날짜별로 카운터를 관리하면서 최대 100개씩 저장
awk -v year="$YEAR" -v max_rows="$MAX_ROWS_PER_FILE" '
BEGIN {
    # 월을 숫자로 변환하는 배열
    months["Jan"]="01"; months["Feb"]="02"; months["Mar"]="03"; months["Apr"]="04";
    months["May"]="05"; months["Jun"]="06"; months["Jul"]="07"; months["Aug"]="08";
    months["Sep"]="09"; months["Oct"]="10"; months["Nov"]="11"; months["Dec"]="12";
}

{
    # 월을 숫자로 변환
    month_str = $1
    day = $2
    month = months[month_str]
    
    # 일자 1자리면 0붙이기
    if (length(day) == 1) day = "0" day
    
    # 파일명 만들기
    filename = sprintf("ssh-%s%s%s.log", year, month, day)
    
    # 해당 날짜의 카운터가 max_rows에 도달하지 않았으면 저장
    if (counts[filename] < max_rows) {
        print $0 >> "'$OUTPUT_DIR'/" filename
        counts[filename]++
    }
    # max_rows에 도달하면 해당 날짜는 더 이상 저장하지 않음
}

END {
    # 처리 결과 출력
    for (filename in counts) {
        printf "📁 %s: %d개 저장됨\n", filename, counts[filename]
    }
}
' "$LOG_FILE"

echo "✅ 각 날짜별로 최대 ${MAX_ROWS_PER_FILE}개씩 로그를 분리했습니다." 