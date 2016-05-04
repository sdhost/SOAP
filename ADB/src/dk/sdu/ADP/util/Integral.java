package dk.sdu.ADP.util;

public class Integral {
	
	static public float calc(int start, int end, int step, float[] f){
		//Calculate Integral for f(x)dx from a to b, f[] store all the value from f(a) to f(b)
		float result = 0.0f;
		if(end < start)
			return result;
		else{
			for(int i=start + step; i <= end; i+= step){
				result += step * (f[i-1] + f[i] ) / 2;
			}
			return result;
		}
	}
}
