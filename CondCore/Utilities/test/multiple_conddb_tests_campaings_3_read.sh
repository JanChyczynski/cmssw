set -euo pipefail

export TEST_CONDDB_COMM_SCHEMA=cms_conditions_test

# for PAYLOAD_SIZE in 10000000; do
#     for PAYLOAD_NUMBER in 1 4 16 32 64 128; do
#         echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
#         # ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --destdb-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_4_s${PAYLOAD_SIZE}_expTo128
#         echo "./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_4_s${PAYLOAD_SIZE}_expTo128  --delete-dest-sqlite"
#     done
# done
# exit 0
# for PAYLOAD_SIZE in 100000; do
#     for PAYLOAD_NUMBER in 1 32 128 512 2048; do
#         echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
#         # ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --destdb-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_5_s${PAYLOAD_SIZE}_expTo2048
#         ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_5_s${PAYLOAD_SIZE}_expTo2048  --delete-dest-sqlite
#     done
# done


for PAYLOAD_SIZE in  1000 10000 100000 1000000 1000000 100000000; do
    for PAYLOAD_NUMBER in 1; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        # ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --destdb-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_n${PAYLOAD_NUMBER}
        ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_n${PAYLOAD_NUMBER}  --delete-dest-sqlite
    done
done
# 
# for PAYLOAD_SIZE in  1000 10000 100000 1000000; do
#     for PAYLOAD_NUMBER in 10; do
#         echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
#         # ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --destdb-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_n${PAYLOAD_NUMBER}
#         ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_n${PAYLOAD_NUMBER}  --delete-dest-sqlite
#     done
# done

for PAYLOAD_SIZE in   1000000 100000000; do
    for PAYLOAD_NUMBER in 10; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        # ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --destdb-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_2${PAYLOAD_NUMBER}
        ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 5 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_2_n${PAYLOAD_NUMBER}  --delete-dest-sqlite
    done
done
