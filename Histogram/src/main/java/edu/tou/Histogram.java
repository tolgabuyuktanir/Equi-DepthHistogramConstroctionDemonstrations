package edu.tou;

import org.primefaces.model.chart.BarChartModel;
import org.primefaces.model.chart.ChartSeries;

import javax.annotation.PostConstruct;
import javax.faces.bean.ManagedBean;
import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.primefaces.model.chart.Axis;
import org.primefaces.model.chart.AxisType;


/**
 * Created by kars on 7/11/16.
 */
@ManagedBean(name = "histogram")
public class Histogram implements Serializable {
    private String numberOfBuckets;
    private Date startDate;
    private Date endDate;
    private String input;
    private String output;

    private BarChartModel model;

    public Histogram() {
            model = new BarChartModel();
            ChartSeries boys = new ChartSeries();
            boys.setLabel("Boys");
            boys.set(100,100);
            boys.set(1700,100);
            boys.set(1457700,100);
            boys.set(145646700,100);
            boys.set(49545609845l,100);


            model.addSeries(boys);

            model.setTitle("Equi Depth Histogram");
            model.setLegendPosition("ne");

            Axis xAxis=model.getAxis(AxisType.X);
            xAxis.setLabel("Number Of Tuples");
            Axis yAxis=model.getAxis(AxisType.Y);
            yAxis.setLabel("N/B");
            yAxis.setTickFormat("-");
    }

    public String message()
    {
        Format formatter = new SimpleDateFormat("yyyyMMdd");
        System.out.println(formatter.format(startDate));
        try {

            // print a message
            System.out.println("Executing notepad.exe");

            // create a process and execute notepad.exe
            Process process = Runtime.getRuntime().exec("ls -la");
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(process.getInputStream()));
            String s = null;
            while ((s = stdInput.readLine()) != null) {
                System.out.println(stdInput);
            }
            // print another message
            System.out.println("Notepad should now open.");
            System.out.println(getNumberOfBuckets());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return "index.xhtml";
    }
    public String getHistogramInformation()
    {
        if(getStartDate()==null && getEndDate()==null)
        {
            return "";
        }
        else
            return "Number Of Buckets:"+getNumberOfBuckets()+" Start Date:"+getStartDate()+" End Date:"+getEndDate()+" Input:"+getInput()+" Output:"+getOutput();
    }
    public String mergeHistograms() throws IOException {

        System.out.println("Histogram Merging...");
        Map<Object,Number> histogramBoundariesMap=new LinkedHashMap<Object, Number>();
        Format formatter = new SimpleDateFormat("yyyyMMdd");
        //hadoop jar /home/kars/Desktop/HistogramEstimate.jar edu.tou.HistogramEstimate 3 10100701 20100703 /Summaries/ output
        String command1="hadoop jar /home/kars/Desktop/HistogramEstimate.jar edu.tou.HistogramEstimate "+ getNumberOfBuckets()+
                " " + formatter.format(getStartDate()) + " " + formatter.format(getEndDate()) + " " + getInput() + " " +getOutput();
        Process process = Runtime.getRuntime().exec(command1);
        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(process.getErrorStream()));
        String s = null;
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
        }
        String command2="hadoop fs -cat /user/kars/output/*";
        Process process2 = Runtime.getRuntime().exec(command2);
        BufferedReader stdInput2 = new BufferedReader(new
                InputStreamReader(process2.getInputStream()));
        String s2 = null;
        while ((s2 = stdInput2.readLine()) != null) {
            histogramBoundariesMap.put(s2,100);
        }
        System.out.println(histogramBoundariesMap);


        model = new BarChartModel();
        ChartSeries equiDepthHistogram = new ChartSeries();
        equiDepthHistogram.setLabel("Values");
        equiDepthHistogram.setData(histogramBoundariesMap);
        model.addSeries(equiDepthHistogram);

        model.setTitle("Equi Depth Histogram");
        model.setLegendPosition("ne");

        Axis xAxis=model.getAxis(AxisType.X);
        xAxis.setLabel("Number Of Tuples");
        Axis yAxis=model.getAxis(AxisType.Y);
        yAxis.setLabel("N/B");
        yAxis.setTickFormat("-");
        return "deneme";
    }

    public String getNumberOfBuckets() {
        return numberOfBuckets;
    }

    public void setNumberOfBuckets(String numberOfBuckets) {
        this.numberOfBuckets = numberOfBuckets;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }
    public BarChartModel getModel() {
        return model;
    }

}
