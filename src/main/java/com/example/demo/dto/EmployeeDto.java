package com.example.demo.dto;

import com.example.demo.entity.Employee;

/**
 * 用于返回到封装前端数据
 * Created by woni on 18/1/24.
 */
public class EmployeeDto extends Employee{

    //用于保存高亮的属性
    private String[] highLightValue;

    public String[] getHighLightValue() {
        return highLightValue;
    }

    public void setHighLightValue(String[] highLightValue) {
        this.highLightValue = highLightValue;
    }
}
