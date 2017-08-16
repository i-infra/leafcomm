cython -3 -D --embed node_core.py 
gcc -fPIC -O2 node_core.c -I/usr/include/python3.5 -L/usr/lib/python3.5 -lpython3.5m -o node_core
