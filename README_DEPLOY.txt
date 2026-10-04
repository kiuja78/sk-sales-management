SK 영업관리 시스템 WEB V1.00
==============================

[새 GitHub 저장소에 직접 업로드]
1. GitHub에서 새 Repository를 생성합니다.
2. GitHub Free에서 Pages를 사용하려면 Public 저장소로 만듭니다.
3. 이 ZIP을 풀고 안의 파일과 assets 폴더를 저장소 루트에 전부 업로드합니다.
4. Commit changes를 누릅니다.
5. Settings > Pages로 이동합니다.
6. Source: Deploy from a branch
7. Branch: main / Folder: /(root)
8. Save 후 생성된 Pages 주소로 접속합니다.

[현재 V1.00 데이터 방식]
웹사이트 자체는 PC/노트북 어디서든 접속할 수 있습니다.
다만 데이터는 SK 전용 LocalStorage + IndexedDB에 저장되므로
각 PC/브라우저 데이터는 서로 자동 동기화되지 않습니다.

여러 PC에서 동일 데이터를 공유하려면 이후 중앙 DB가 필요합니다.
필요할 때 Supabase를 단계별로 천천히 연결할 수 있습니다.

[분리 원칙]
쿠쿠 원격 데이터 서버/라이선스 서버에 자동 연결하지 않습니다.
SK 저장키, IndexedDB, 캐시, 백업 식별자는 쿠쿠와 분리되어 있습니다.
