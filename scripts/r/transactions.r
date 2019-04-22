library(ggplot2)
library(reshape)
library(stringr)
library(igraph)

minX <- as.integer(commandArgs(TRUE)[1])
maxX <- as.integer(commandArgs(TRUE)[2])

source(paste(Sys.getenv('R_SCRIPTS_PATH'), 'annotation.r', sep='/'))
df2 <- load_annotations()

if(file.exists("trades_cumulative.csv")){
	df <- read.csv("trades_cumulative.csv", sep=",")
	df$time <- df$time / 1000.0
    print(df)
	
	p <- ggplot(df) + theme_bw()
	p <- add_annotations(p, df, df2)
	p <- p + geom_line(aes(time, trades), colour='2')
	p <- p + theme(legend.position="bottom", legend.direction="horizontal")
	p <- p + labs(x = "\nTime into experiment (Seconds)", y = "Total trades completed\n")
	p <- p + xlim(minX, maxX)
	p
	
	ggsave(file="trades.png", width=8, height=6, dpi=100)
}

# Draw the transaction graph
pdf(NULL)

library("igraph")

data = read.csv("trades.log", sep=",")
data_matrix = data.matrix(data[,4:5])

g <- graph_from_edgelist(data_matrix, directed=F)

png(filename = "transaction_graph.png", width=1000, height=1000)

plot(g, layout=layout.fruchterman.reingold(g, niter=10000), vertex.size=8, edge.width=5)
title("Trades", cex.main=3)

dev.off()
