#include <iostream>
int main() {
    std::string s = "select * from sbtest";
    auto index = s.find("select");
    std::cout << index << std::endl;
    return 0;
}
