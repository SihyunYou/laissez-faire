### ● 플래그 및 수익성

|  | 위험성 | -z | -t | -x | -m | -v | 수익성 |
| ------ | ----------- | ------: | ------: | ------: | ------: | ------: | ------: |
| 1.bat | 매우 높음 | 1.8 | 10 | 2048(2^10) | 1 | 0.38 | 월 5.5~6.5%
| 2.bat | 높음 | 1.9 | 11 | 4096(2^11) | 1.1 | 0.36 | 월 4.5~5.5%
| 3.bat | 중간 | 2 | 12 | 8192(2^12) | 1.2 | 0.34 | 월 3.5~4.5%
| 4.bat | 낮음 | 2.1 | 13 | 16384(2^13) | 1.3 | 0.32 | 월 2.5~3.5%
| 5.bat | 매우 낮음 | 2.2 | 14 | 32768(2^14) | 1.4 | 0.3 | 월 1.5~2.5%
###### 기준 투입액(-s) : 유동, 분봉(-n) : 5, 파편화(-f) : 3

## 
* -s : 투입할 총액. 미설정 시, 업비트에 있는 총 보유KRW이 투입된다.
* -n : 밴드를 관찰할 분봉의 값. (ex. 1, 3, 5, 10, 15, 30...)
* -z : 볼린저 밴드의 표준정규분포 곱상수. 값이 커질수록 저점에서 매수할 수 있으나 매수포착 기회가 줄어든다.
* -t : 최종 매수 지점의 상위비율. (ex. t가 6이라면, 첫 분할매수액의 94퍼까지 분할매수주문을 요청한다.)
* -f : 1% 당 분절할 분할매수주문 개수. (ex. f가 4라면, 0.25%의 비율로 분할매수한다.)
* -x : 분할매수할 주문액의 증가비율. 값이 커질수록 매수평균가가 저점에서 형성된다.
* -m : 매수 제한 밴드 폭의 길이. 값이 커질수록 저점에서 매수할 수 있으나 매수포착 기회가 줄어든다.
* -v : 매도 지점 비율. (ex. -v가 0.5이고 매수평균가가 1,000이라면 매도지점은 1005이다.)


##
###### * 위 스크립트를 그대로 돌리시면 확정손해를 보게 됩니다. 관심이 있으신 분은 caesar2937@gmail.com 으로 문의주세요.
