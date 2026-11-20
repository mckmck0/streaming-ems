# Spike: Restate jako runtime dla EMS prosumenckiego

Cel: sprawdzić w krótkim czasie, czy Restate udźwignie cztery mechanizmy z tematu pracy:

1. wiele strategii na tym samym strumieniu, z dodawaniem w trakcie działania,
2. zamkniętą pętlę sprzężenia między encjami,
3. trwałe timery,
4. skalę floty.

Kryterium twarde: rozliczenie każdej strategii **identyczne co do bitu** z symulacją offline, także po `kill -9` usługi i serwera.

## Wersje i środowisko testów

restate-server 1.7.12 i CLI 1.7.12 (npm), restate-sdk (Python) 1.0.5, Python 3.12, hypercorn, httpx.
Testy wykonano na **1 vCPU i 4 GB RAM**, gdzie serwer, usługi i generator obciążenia dzielą jeden rdzeń.
Przepustowość to więc dolna granica dla tej konfiguracji, a nie benchmark.

## Architektura

| Encja (Virtual Object) | Klucz | Stan | Rola |
|---|---|---|---|
| `Household` | ID gospodarstwa | lista aktywnych strategii | router: każde zdarzenie trafia do wszystkich strategii (wysyłka po nazwie usługi) i do pętli na żywo |
| `NoBattery`, `SelfConsumption`, `PriceAware`, `SelfConsumptionReserve` | ID gospodarstwa | wirtualny SoC, rozliczenie, plan dnia | strategie kontrfaktyczne |
| `Registry` | `fleet` | lista gospodarstw i strategii | komunikat sterujący zmieniający routing w locie (odpowiednik broadcast state) |
| `Ems` → `Battery` → `Ems`, plus `Meter` | ID gospodarstwa | SoC widziany, SoC fizyczny, rachunek | zamknięta pętla na komunikatach jednokierunkowych |
| `Tariff` | strefa, np. `PL` | krzywa day-ahead, log odświeżeń | serwis cen: shared handler do odczytu, samoplanujące się odświeżanie ze strażnikiem generacji |

Pliki:

| Plik | Zawartość |
|---|---|
| `ems_spike/model.py` | czysta logika: generator syntetyczny, model baterii (η, limity SoC i mocy), strategie, symulacja referencyjna |
| `ems_spike/restate_app.py` | wdrożenie #1 (port 9080). Opcjonalnie `EMS_RETRY_MAX_INTERVAL_S` ogranicza backoff ponowień |
| `ems_spike/extra_app.py` | wdrożenie #2 (port 9081) z nową strategią |
| `loadgen.py` | wprowadzanie zdarzeń z kluczami idempotencji, odczyt stanu przez SQL, weryfikacja bit po bicie |
| `experiments.py`, `rerun.py` | scenariusze A–E oraz czyste powtórki B, D, D2 |
| `ctl.sh`, `tools/killport.py` | start i `kill -9` serwera oraz usług. Usługi zabijane po porcie, bo hypercorn obsługuje żądania z procesu potomnego |
| `results.json` | wyniki obowiązujące (z czystych powtórek) |
| `results_run1.json` | pierwsze przebiegi z błędnym zabijaniem usług, zachowane dla przejrzystości |
| `guard_test.txt` | wynik testu strażnika generacji |

## Uruchomienie (Linux lub macOS)

```bash
npm ci                                   # restate-server i CLI 1.7.12 wg package-lock.json
pip install -r requirements.txt
./ctl.sh wipe; ./ctl.sh server; sleep 5
./ctl.sh svc1; sleep 3; ./ctl.sh register http://127.0.0.1:9080
python3 experiments.py A C E:200         # poprawność, routing w locie, skala
python3 rerun.py                         # B (awarie), D i D2 (timery po awarii)
```

Stan i wywołania można podglądać w UI Restate (port administracyjny 9070) albo SQL-em, np.:

```bash
./ctl.sh sql "select service_key, value_utf8 from state where service_name='PriceAware' limit 5"
```

## Wyniki

| Test | Wynik |
|---|---|
| **A**: 50 gospodarstw × 96 interwałów 15-min, 3 strategie + pętla | 4 800 zdarzeń, 38 400 wywołań handlerów w 82,2 s (~467/s). 150/150 stanów zgodnych z referencją. 0 zdarzeń poza kolejnością. Rachunek z pętli `Ems→Battery→Meter` równy rachunkowi `SelfConsumption` w 50/50 gospodarstw |
| **B**: jak A, plus `kill -9` usługi SDK (przy 1 450 zdarzeniach) i serwera Restate (przy 2 886) | 543 ponowienia po stronie generatora. 150/150 stanów zgodnych. Brak duplikatów i strat (n = 96 wszędzie). 0 poza kolejnością. 135,7 s |
| Ponowne wysłanie całego dnia z tymi samymi kluczami idempotencji, z `kill -9` obu komponentów | 0 duplikatów |
| **C**: nowa strategia wdrożona w połowie strumienia | wdrożenie #2 zarejestrowane bez restartu. 50/50 gospodarstw dołączyło nową strategię w biegu. 200/200 stanów zgodnych (nowa strategia od własnego `first_t`) |
| **D**: timery 6 s i 20 s oraz odświeżanie co 3 s, `kill -9` serwera i obu usług, przerwa 9,2 s, domyślne ponowienia | oba timery odpalone, nic nie zginęło. Opóźnienie względem terminu: +22,8 s i +8,7 s. Największa przerwa w odświeżaniu 22,4 s |
| **D2**: jak D, ponowienia ograniczone do ≤ 1 s, przerwa 8,9 s | +28,8 s i +14,8 s. Odświeżanie wznowione (7 tików po restarcie), największa przerwa 13,5 s |
| **E**: 200 gospodarstw | 19 200 zdarzeń, 153 600 wywołań w 314,9 s (~488/s, stała przepustowość: limit jednego rdzenia). 600/600 stanów zgodnych |
| Strażnik generacji w `Tariff` | bez strażnika: start/stop/start daje dwa łańcuchy (10 tików w 9 s, odstępy 0,07/1,93 s). Ze strażnikiem: jeden przestarzały tik, potem jeden łańcuch (odstępy 2,01 s) |

Średni koszt dnia na gospodarstwo w scenariuszu A (dane syntetyczne):

| Strategia | PLN/dzień |
|---|---|
| `NoBattery` | 6,96 |
| `SelfConsumption` | −0,53 |
| `PriceAware` | 2,27 |

Ta sama kolejność strategii wychodzi w B, C i E dla innych gospodarstw.

## Wnioski

1. **Mechanizmy 1–3 działają na gotowych prymitywach Restate.** Exactly-once potwierdzone po `kill -9` zarówno usługi SDK, jak i serwera: rozliczenia są identyczne co do bitu z symulacją offline. Ten test warto przenieść do pracy wprost.
2. **Ewaluacja kontrfaktyczna daje treść merytoryczną.** Naiwne „ładuj o 2:00” przegrywa z autokonsumpcją. Prawdopodobna przyczyna: magazyn pełny po nocy nie przyjmuje południowej nadwyżki PV, która trafia do sieci po niskiej cenie sprzedaży (asymetria net-billingu). To motywacja dla prognoz i MPC.
3. **Dwa zegary.** W przyspieszonym odtwarzaniu pętla `Ems` na żywo w ~99% decyzji nie miała jeszcze potwierdzenia SoC z poprzedniego kroku (średnie opóźnienie kolejki 23 s w A, 41 s w E). Strategie kontrfaktyczne trzymają więc model baterii we własnym stanie: wynik jest deterministyczny. Pętla między encjami jest do trybu na żywo.
4. **Timery nie giną, ale po pełnym restarcie startują z opóźnieniem.** W każdym przebiegu oba timery odpaliły się w tej samej chwili, około 18–24 s po restarcie. Wskazuje to na stałą fazę odtwarzania, a nie na dryf pojedynczych timerów.
   - Ograniczenie ponowień tego nie skróciło. W logu serwera są tylko dwa krótkie ponowienia wywołań (0,6 s i 1,3 s).
   - Pojawia się natomiast komunikat o ponawianiu odczytu z logu replikowanego. Hipoteza do weryfikacji: po `kill -9` serwer odtwarza log, a `/restate/health` odpowiada OK, zanim partycje są gotowe.
   - Przy kroku 15 min to nie jest krytyczne, ale w pracy to metryka do zmierzenia (recovery latency).
5. **Wzorzec „cron” wymaga strażnika generacji.** Jest dodany w `Tariff`.
6. **Pułapki:**
   - request-response w cyklu między handlerami wyłącznymi grozi zakleszczeniem,
   - nie usypiać handlera wyłącznego,
   - `register --force` używać tylko w trybie deweloperskim,
   - zabijać usługi po porcie, nie po nazwie procesu.

## Ograniczenia i kolejne kroki

Ograniczenia: 1 vCPU, pojedynczy węzeł Restate, ingress HTTP zamiast Kafki, dane syntetyczne, proste strategie. Wymiaru skali w sensie pracy tu nie zmierzono.

Kolejne kroki:
1. Subskrypcja Kafki z kluczem rekordu równym ID gospodarstwa.
2. Dane ENTSO-E i Open-Meteo.
3. Czas symulowany niesiony przez zdarzenia.
4. Strategia MPC.
5. Pomiary na wielu rdzeniach i węzłach.
6. Rozbicie czasu odtwarzania po awarii na fazy.
