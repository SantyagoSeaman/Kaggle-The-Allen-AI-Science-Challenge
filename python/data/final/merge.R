# 0 -- 8000 -- 10000 -- 18000
final_21200 <- read.csv("21200--final_2016-02-09-10-26-24.csv", stringsAsFactors=FALSE)

final_3500 <- read.csv("--final_3500_2016-02-09-11-04-54.csv", stringsAsFactors=FALSE)
final_11600 <- read.csv("--final_11600_2016-02-09-11-10-26.csv", stringsAsFactors=FALSE)
final_13600 <- read.csv("--final_13600_2016-02-09-11-15-47.csv", stringsAsFactors=FALSE)

final_4900 <- read.csv("--final_4900_2016-02-09-15-46-18.csv", stringsAsFactors=FALSE)
final_13000 <- read.csv("--final_13000_2016-02-09-15-46-17.csv", stringsAsFactors=FALSE)
final_15100 <- read.csv("--final_15100_2016-02-09-16-01-04.csv", stringsAsFactors=FALSE)

final_5600 <- read.csv("--final_5600_2016-02-09-18-57-07.csv", stringsAsFactors=FALSE)
final_13700 <- read.csv("--final_13700_2016-02-09-18-55-30.csv", stringsAsFactors=FALSE)
final_15800 <- read.csv("--final_15800_2016-02-09-18-59-38.csv", stringsAsFactors=FALSE)

final_10300 <- read.csv("--final_10300_2016-02-10-10-24-29.csv", stringsAsFactors=FALSE)
final_18400 <- read.csv("--final_18400_2016-02-10-10-23-19.csv", stringsAsFactors=FALSE)
final_20500 <- read.csv("--final_20500_2016-02-10-10-25-20.csv", stringsAsFactors=FALSE)


period <- 18001:18400

View(final_18400[period, ])
View(final_21200[period, ])
View(submission_sample_phase2[period, ])

zzz <- final_18400[period, 'correctAnswer'] != final_21200[period, 'correctAnswer']
zzz[zzz == T]


