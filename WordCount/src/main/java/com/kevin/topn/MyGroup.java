package com.kevin.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MyGroup extends WritableComparator
{

    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {

        OrderBean orderBean1 = (OrderBean) a;

        OrderBean orderBean2 = (OrderBean) b;


        String userId1 = orderBean1.getUserid();

        String userId2 = orderBean2.getUserid();


        int compare = userId1.compareTo(userId2);

        if (compare == 0)
        {
            return orderBean1.getDatetime().compareTo(orderBean2.getDatetime());
        }
        else
        {
            return compare;
        }
    }
}
