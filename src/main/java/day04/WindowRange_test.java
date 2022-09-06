package day04;

public class WindowRange_test {
    public static void main(String[] args) {
        //窗口的起始和结束时间 [) 必须是窗口长度整数倍
        long inputTime = 1000;
        long windowSize = 5000;
        long startTime = inputTime - (inputTime % windowSize);
        long endTime = inputTime - (inputTime % windowSize) + windowSize;
        System.out.println("[" + startTime + "," + endTime + ")");
    }
}
