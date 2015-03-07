package hadoopprjt3;




import java.io.*;


class filedivider
{ 
	public static void main(String[] args)
   {
	
		BufferedReader br=null;
		
		BufferedWriter brw =null;
		int filenum=0;

		int linenum=0;
		try{
			String line;
			br=new BufferedReader(new FileReader(args[0]));
			brw=new BufferedWriter(new FileWriter(args[0]+"part_"+filenum));
			while((line=br.readLine())!=null){
				linenum++;
				brw.write(line);
				brw.newLine();
				if(linenum==35){
					filenum++;
					linenum=0;
					brw.close();
					brw=new BufferedWriter(new FileWriter(args[0]+"part_"+filenum));
				}
			}
			br.close();
			brw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
   }
}
