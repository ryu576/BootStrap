package bootStrapping;
import com.sun.xml.internal.org.jvnet.fastinfoset.RestrictedAlphabet;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class bootStrapping {
    
    
    public static int numsims=1000;
    public static void main(String[] args) {
	String[] brands = {"188705.0","156699.0"};
        String[] periods = {"2.20120101E8","2.20120101E8"};
        String[] demos = {"21.0","125.0","155.0"};
        Connection conn = null;Statement st = null;ResultSet rstru = null;ResultSet rsfb = null;
        long startTime = System.currentTimeMillis();
        HashMap<Long,Integer> truhits = new HashMap<Long,Integer>();HashMap<Long,Integer> fbhits = new HashMap<Long,Integer>();
        Double[][][] rfs = new Double[brands.length][periods.length][demos.length];for(int i=0;i<brands.length;i++)for(int j=0;j<periods.length;j++)for(int k=0;k<demos.length;k++)rfs[i][j][k]=0.0;
        Long[] rnidsarr;
        try {
            Class.forName("org.netezza.Driver");System.out.println(" Connecting ... ");
            conn = DriverManager.getConnection(url, user, pwd); System.out.println(" Connected "+conn);
            st = conn.createStatement();
            for(int i=0;i<brands.length;i++){
                for(int j=0;j<periods.length;j++){
                    for(int k=0;k<demos.length;k++){
                        ArrayList<Long> rnids= new ArrayList<Long>();
                        rstru = st.executeQuery("select sum(HITS),RN_ID from SCRATCH..mscijg_ocr_bstrap_af_input where PERIOD_ID = " + periods[j] + " and BRAND_ID = " + brands[i] +" and TRUTH_DEMO_ID = " + demos[k]+ " group by RN_ID ");
                        while (rstru.next()) {//Get query results and put them into a hash map.
                            String temp0 = rstru.getString(1);String temp1 = rstru.getString(2);
                            Double tmpdoub = Double.parseDouble(temp0);
                            Integer tmpint = tmpdoub.intValue();
                            tmpdoub = Double.parseDouble(temp1);
                            Long tmplong = new Long(tmpdoub.longValue());//All this drama just to store these values to java objects.. I sense there is scope for optimization
                            if(truhits.containsKey(tmplong))truhits.put(tmplong,truhits.get(tmplong)+tmpint);//This is where the insertion happens. Update if already present.
                            else{
                                truhits.put(tmplong,tmpint);
                                rnids.add(tmplong);//Also populate an array of unique RNIDs as arrays are a lot faster to iterate than HashMaps.
                            }//Add if not.
                        }
                        rnidsarr = new Long[rnids.size()];//We should be sampling from the RNIds of that entire period.
                        rnidsarr =  rnids.toArray(rnidsarr);
                        rstru = st.executeQuery("select sum(FB_HITS),RN_ID from SCRATCH..mscijg_ocr_bstrap_af_input where PERIOD_ID = " + periods[j] + " and BRAND_ID = " + brands[i] +" and FB_DEMO_ID = " + demos[k]+ " and FB_DEMO_ID IS NOT null group by RN_ID ");
                        while (rstru.next()) {//Get query results and put them into a hash map.
                            String temp0 = rstru.getString(1);String temp1 = rstru.getString(2);
                            Double tmpdoub = Double.parseDouble(temp0);
                            Integer tmpint = tmpdoub.intValue();
                            tmpdoub = Double.parseDouble(temp1);
                            Long tmplong = new Long(tmpdoub.longValue());//All this drama just to store these values to java objects.. I sense there is scope for optimization
                            if(fbhits.containsKey(tmplong))fbhits.put(tmplong,fbhits.get(tmplong)+tmpint);//This is where the insertion happens. Update if already present.
                            else fbhits.put(tmplong,tmpint);//Add if not.
                        }
                        rfs[i][j][k] = computerfs(truhits,fbhits,rnidsarr);
                    }
                }
            }
        }catch(Exception e){e.printStackTrace();}
        finally{
            try {
                if( rstru != null)rstru.close();if( st!= null)st.close();if( conn != null)conn.close();
            } catch (SQLException e1){e1.printStackTrace();}
        }
        long finishTime = System.currentTimeMillis();
        System.out.println("The program took: "+(finishTime-startTime)+ " ms to run");
        System.out.println("All Done!");
    }
    public static double computerfs(HashMap<Long,Integer> truhits,HashMap<Long,Integer>fbhits,Long[] rnidsarr){
       double sqdev=0.0, meanrf=0.0;
                        double tmptru=0.0,tmpfb=0.0;
                        for(int j1=0;j1<rnidsarr.length;j1++){//First calculate the mean of the adjustment factor (no boot strapping required for that)
                            if(truhits.containsKey(rnidsarr[j1]))tmptru+=truhits.get(rnidsarr[j1]);
                            if(fbhits.containsKey(rnidsarr[j1]))tmpfb+=fbhits.get(rnidsarr[j1]);
                        }
                        if(tmpfb>0)meanrf = tmptru/tmpfb;
                        
                        for(int i1=0;i1<numsims;i1++){//Next we do the actual boot strap across the number of samples and calculate the average square error from the mean calculated before.
                            Long[] sample = samplewreplacement2(rnidsarr);
                            Double tru=0.0,fb=0.0;
                            for(int j1=0;j1<sample.length;j1++){
                                if(truhits.containsKey(sample[j1]))tru+=truhits.get(sample[j1]);
                                if(fbhits.containsKey(sample[j1]))fb+=fbhits.get(sample[j1]);//Aggregate the number of hits across respondents.
                            }
                            if(fb>0)sqdev += Math.pow((tru/fb-meanrf),2);
                        }
        return(sqdev/numsims);
    }
    
    public static String[] execquery(String sql){
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        String[] res=null;
     
        try {
            Class.forName("org.netezza.Driver");
            System.out.println(" Connecting ... ");
            conn = DriverManager.getConnection(url, user, pwd);
            System.out.println(" Connected "+conn);
            st = conn.createStatement();
            for(int i=0;i<100;i++){
            rs = st.executeQuery(sql);
            List rowValues = new ArrayList();
            List rowValues2 = new ArrayList();
            List rowValues3 = new ArrayList();
            while (rs.next()) {
                rowValues.add(rs.getString(1));
                //rowValues2.add(rs.getString(2));
                //rowValues3.add(rs.getString(3));
            }
// You can then put this back into an array if necessary
        res = (String[]) rowValues.toArray(new String[rowValues.size()]);
        }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if( rs != null)rs.close();
                if( st!= null)st.close();
                if( conn != null)conn.close();
            } catch (SQLException e1){e1.printStackTrace();}
        }
    
    //return (Long[]) rowVals.toArray(new Long[rowVals.size()]);
        return(res);
    }
    
    public static HashMap<Long,Integer> hash(Long[] keys,Integer[] vals){
        HashMap<Long,Integer> words_fre = new HashMap<Long,Integer>();
        for(int i=0;i<keys.length;i++){
            if(words_fre.containsKey(keys[i]))words_fre.put(keys[i],words_fre.get(keys[i])+vals[i]);
            else words_fre.put(keys[i],vals[i]);
        }
        return(words_fre);
    }
    public static HashMap<Long,Integer> hash(Long[] keys){
        HashMap<Long,Integer> words_fre = new HashMap<Long,Integer>();
        int rnid=0;
        for(int i=0;i<keys.length;i++){
        if(words_fre.containsKey(keys[i])){
            int a = words_fre.get(keys[i]);
            words_fre.put(keys[i],a+1);
        }
        else{
            words_fre.put(keys[i],1);
            rnid++; // unique respondents count
          }
        }
        return(words_fre);
    }
    //Leaving this in as it is an alternative to the array of longs I implement next. I figured that looping through a Hash map would be a lot slower than looping though an array and that this would compensate even for the fact that we are loooping only through the unique elements for the case of a hash map. Didn't test this out though.
    public static HashMap<Long,Integer> samplewreplacement(Long[] resps){
        HashMap<Long,Integer> reslt = new HashMap<Long,Integer>();
        Random rn = new Random();
        for(int i=0;i<resps.length;i++){
            int j = rn.nextInt(resps.length);
            if(reslt.containsKey(resps[j])){
                int a = reslt.get(resps[j]);
                reslt.put(resps[j],a+1);
            }
            else reslt.put(resps[j],1);
        }
        return(reslt);
    }
    
    public static Long[] samplewreplacement2(Long[] resps){
        Long[] reslt = new Long[resps.length];
        Random rn = new Random();
        for(int i=0;i<resps.length;i++){
            int j = rn.nextInt(resps.length);//Generate an independent integer to index the input array.
            reslt[i] = resps[j];
        }
        return(reslt);
    }
    
    public static Double compute(int brand,int demo){
        Double d1=0.0;
        Double fbpvs,truthpvs;
        //First run the queries .. 
        String[] temp = execquery("SELECT HITS FROM SOMETABLE WHERE BRAND_ID= " + brand + " AND TRUTH_DEMO_ID= "+demo);//There are two demo columns.. FB_DEMO
        Integer[] hits = new Integer[temp.length];for(int i=0;i<temp.length;i++)hits[i]=Integer.valueOf(temp[i]);
        temp = execquery("SELECT FB_HITS FROM SOMETABLE WHERE BRAND_ID= " + brand + " AND TRUTH_DEMO_ID= "+demo);//There are two demo columns.. FB_DEMO
        Integer[] fbhits = new Integer[temp.length];for(int i=0;i<temp.length;i++)fbhits[i]=Integer.valueOf(temp[i]);
        temp = execquery("SELECT RN_ID FROM SOMETABLE WHERE BRAND_ID= " + brand + " AND TRUTH_DEMO_ID= "+demo);//There are two demo columns.. FB_DEMO
        Long[] rnids = new Long[temp.length];for(int i=0;i<temp.length;i++)rnids[i]=Long.valueOf(temp[i]);
        HashMap<Long,Integer> hits1 = new HashMap<Long,Integer>();//These should be computed using the GROP_BY command
        HashMap<Long,Integer> fbhits1 = new HashMap<Long,Integer>();//These should be computed using the GROUP_BY command
        List<Long> uniqrnids = new ArrayList<Long>();
        for(int i=0;i<rnids.length;i++){
            if(hits1.containsKey(rnids[i]))hits1.put(rnids[i],hits1.get(rnids[i])+hits[i]);
            else{
                hits1.put(rnids[i],hits[i]);
                uniqrnids.add(rnids[i]);
            }
        }
        fbhits1 = hash(rnids,hits);
        Long[] uniqrnidsArr = (Long[]) uniqrnids.toArray();
        Double[] rf1 = new Double[numsims];
        for(int i=0;i<numsims;i++){
            HashMap<Long,Integer> sampledppl = samplewreplacement(uniqrnidsArr);
            double sumTruHits = 0.0;
            double sumFBHits = 0.0;
            for(Long key : sampledppl.keySet()){
                Integer i1 = sampledppl.get(key);
                sumTruHits += i1*hits1.get(key);
                sumFBHits += i1*fbhits1.get(key);
            }
            rf1[i] = sumTruHits/sumFBHits;
        }
        
        return(d1);
    }
    
    
    
}


