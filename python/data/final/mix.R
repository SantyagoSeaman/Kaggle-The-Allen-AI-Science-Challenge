# 0 -- 8000 -- 10000 -- 18000
result <- submission_sample_phase2

result[18001:nrow(result), 'correctAnswer'] <- final_21200[18001:nrow(result), 'correctAnswer']


result[1:3500, 'correctAnswer'] <- final_3500[1:3500, 'correctAnswer']
result[8001:11600, 'correctAnswer'] <- final_11600[8001:11600, 'correctAnswer']
result[10001:13600, 'correctAnswer'] <- final_13600[10001:13600, 'correctAnswer']

result[3501:4900, 'correctAnswer'] <- final_4900[3501:4900, 'correctAnswer']
result[11601:13000, 'correctAnswer'] <- final_13000[11601:13000, 'correctAnswer']
result[13601:15100, 'correctAnswer'] <- final_15100[13601:15100, 'correctAnswer']

result[4901:5600, 'correctAnswer'] <- final_5600[4901:5600, 'correctAnswer']
result[13001:13700, 'correctAnswer'] <- final_13700[13001:13700, 'correctAnswer']
result[15101:15800, 'correctAnswer'] <- final_15800[15101:15800, 'correctAnswer']

result[5601:10300, 'correctAnswer'] <- final_10300[5601:10300, 'correctAnswer']
result[13701:18400, 'correctAnswer'] <- final_18400[13701:18400, 'correctAnswer']
result[15801:20500, 'correctAnswer'] <- final_20500[15801:20500, 'correctAnswer']

write.csv(result, 'results/mixed_1', dec=".", col.names=TRUE, row.names=FALSE, quote = FALSE)
