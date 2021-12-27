본 저장소의 코드는 클래스 101모든 수업을 마쳤을 때 사용하시는 것을 권장 드립니다. 15챕터를 듣기 전에 받으신 경우 AI 챕터에서 배치 파일을 가상환경으로 설정 하므로 콜렉터, 트레이더 배치파일을 기존의 7-1챕터에서 제공 드리는 배치파일을 사용하시기 바랍니다.

## 사용 방법

- `library/cf.py.bak`을 `cf.py`로 이름 변경 후 *db_ip, db_id, db_passwd, db_port, imi1_accout, imi1_simul_num, start_daily_buy_list, dart_api_key* 값을 본인의 설정에 맞게 변경 하시거나
기존에 사용하던 `cf.py`를 library폴더 안에 복사 붙여넣기 해주시기 바랍니다.

- etf 사용 시
 `cf.py`에서 `use_etf = True` 로 설정

- 추가 스크립트
 - `fetch_etf.py` :  etf 사용 시 daily_buy_list를 삭제하지 않고 기존에 만들어진 daily_buy_list 날짜 테이블에 etf 종목을 추가해 주는 스크립트
         ** library/collector_api.py 1.3.13 버전 이후로는 `cf.py`에서 `use_etf = True` 로 설정만 해도 자동으로
         daily_craw, daily_buy_list, min_craw 에 etf 종목을 업데이트하도록 패치 되었으므로 fetch_etf.py은 사용하지 않아도 됩니다.

 - `delete_rows_in.py` : 콜렉터를 실수로 장중에 돌리셔서 부정확한 데이터가 들어오게 될 경우 특정 날짜 이후로의 데이터를 daily_craw, daily_buy_list에서 모두 지워주는 스크립트

 - `batch_generator.py` :  모든 배치파일 자동 생성기.
         batch_generator는 사용자의 Anaconda 경로에 맞게 배치파일을 자동 생성해 주는 스크립트입니다. 아래 링크로 들어가신 후 다운로드 버튼을 누르시기 바랍니다.
         만약 가상환경을 사용 중이시고 가상환경 명이 수업에서처럼 py37_32, py37_64가 아니라면 32bit와 64bit 가상환경 명을 물어볼 수 있습니다. 각각 맞는 가상환경 명을 적어주시면 됩니다.
         완료 메시지가 뜨면 collector.bat 과 trader.bat등을 실행하시면 됩니다.
         만약 이래도 안된다면 윈도우 컴퓨터나 유저 이름에 한글 혹은 특수문자가 섞여 있어서 생기는 문제일 수 있습니다.

    링크 참고(https://wikidocs.net/85842#bat-batch_generatorpy)