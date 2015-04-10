package edu.cornell.cs.weijia.bs;

import java.util.UUID;

interface interface1
{
   void a();
   void b();
}

abstract class A implements interface1
{
   public void a()
	{
		System.out.println("In a() of A");
		b();
	}
   public void b(){
	   System.out.println("in super class");
   }
}

class A1 extends A
{
	
 	public void b()
	{
 		System.out.println("In B of A1");
 		super.b();
	}
 	
 	
}

public class Try {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		A temp=new A1();
		temp.b();
		UUID a= UUID.randomUUID();
		System.out.println(a.toString().getBytes().length);
		
	}

}
