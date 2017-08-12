register /home/kars/ResearchStudies/EquiDepthHistogramConstructionWithQualityQuaranties/ApachePig/HistogramSummarize.jar;
define Hist HistogramSummarize('10');
A = load '/home/kars/ResearchStudies/EquiDepthHistogramConstructionWithQualityQuaranties/pagestat/20160112' using PigStorage(' ') as (lang:chararray,url:chararray,req:long,byte:long);
B = foreach A generate byte as (byte:long);
C = group B all;
D = foreach C {
sorted = order B by byte;
generate Hist(sorted);
};
store D into 'Summaries/20160111';
