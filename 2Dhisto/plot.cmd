set size square
set title '2D histo'
set xlabel 'PageRank'
set ylabel 'nr of triangles'
set xrange[0:100]
set yrange[0:1000]
set pm3d map
set palette defined (0 "white", 0.100 "blue", 0.250 "green", 1.7500 "red")
splot 'histogram.dat' notitle matrix

