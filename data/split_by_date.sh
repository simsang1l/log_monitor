#!/bin/bash

LOG_FILE="data/SSH.log"
OUTPUT_DIR="data"
YEAR="2024"  # 원하는 연도로 수정

mkdir -p "$OUTPUT_DIR"

awk -v year="$YEAR" '{
    # 월을 숫자로 변환
    month_str = $1
    day = $2
    months["Jan"]="01"; months["Feb"]="02"; months["Mar"]="03"; months["Apr"]="04";
    months["May"]="05"; months["Jun"]="06"; months["Jul"]="07"; months["Aug"]="08";
    months["Sep"]="09"; months["Oct"]="10"; months["Nov"]="11"; months["Dec"]="12";
    month = months[month_str]
    # 일자 1자리면 0붙이기
    if (length(day) == 1) day = "0" day
    # 파일명 만들기
    filename = sprintf("ssh-%s%s%s.log", year, month, day)
    print $0 >> "'$OUTPUT_DIR'/" filename
}' "$LOG_FILE"