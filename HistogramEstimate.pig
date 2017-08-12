register HistogramEstimate.jar;
define Hist HistogramEstimate('3');
A = load 'Summaries/2016010[5-6]/*' as (B: bag {T: tuple(bound:long,numberOfTuple:long)});
B = foreach A generate flatten(B);
C = group B all;
D = foreach C{
sorted = order B by bound;
generate Hist(sorted);
};
dump D;
