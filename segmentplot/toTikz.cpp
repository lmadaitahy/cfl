#include <cstdio>
#include <map>
#include <set>
#include <vector>
#include <utility>
#include <cassert>
#include <string>
#include <algorithm>

using namespace std;



const int paperWidth = 8; 
const double yScale = 0.08;



typedef long long int ll;
typedef pair<int,int> bagid; // cflSize, opID

struct input_line {
  ll time;
  char sechar;
  int cflSize, opID;
};

const ll nostart = -1;

struct baginfo {
  bagid id;
  ll start, end;
  
  baginfo():start(nostart) {}
  baginfo(bagid id, ll start, ll end):id(id),start(start),end(end) {}
};

ll minTime = 1000000000000000LL, maxTime = -1;

double scale(ll time) {
  assert(maxTime>minTime);
  assert(time>0);

  double mi = minTime, ma = maxTime, t = time;
  return (t-mi)/(ma-mi)*paperWidth;
}

struct boperator {
  int id;
  string name;
  int color;

  boperator(int id, string name, int color): id(id), name(name), color(color) {}
};

vector<boperator> ops{
  boperator(0,"vertices0",0),
  boperator(1,"vertices (distinct)",8),
  boperator(2,"labels\\_0",2),
  boperator(3,"updates\\_0",3),
  boperator(150,"create MutableBag",12),
  boperator(5,"updates\\_1 (Phi)",4),
  boperator(6,"msgs (join with edges)",5),
  boperator(7,"minMsgs (reduceByKey)",6),
  boperator(1500,"join with MutableBag",7),
  boperator(15000,"update of MutableBag",1),
  boperator(10,"combiner of nonEmpty",9),
  boperator(11,"exit-condition",10),
  boperator(16,"nonEmpty",11),
};


int main() {

  map<int,int> color;
  for(boperator &op: ops){
    color[op.id]=op.color;
  }

  // http://colorbrewer2.org/#type=qualitative&scheme=Paired&n=12
  const char *colordefs[] = {"A6CEE3","1F78B4","B2DF8A","33A02C","FB9A99","E31A1C","FDBF6F","FF7F00","CAB2D6","6A3D9A","FFFF99","B15928","000000"};
  for(int i=0; i<13; i++){
    printf("  \\definecolor{mycolor%d}{HTML}{%s}\n",i,colordefs[i]);
  }
  printf("\n");


  vector<input_line> inputLines;

  while(1){
    inputLines.emplace_back();
    input_line &cl = inputLines[inputLines.size()-1];

    int r = scanf("%lld ", &cl.time);
    if (r == EOF) {inputLines.pop_back(); break;}
    scanf("%c ", &cl.sechar);
    scanf("%d ", &cl.cflSize);
    scanf("%d\n", &cl.opID);
  }

  map<bagid,baginfo> fromid;
  map<int,set<bagid>> cflSizes;

  for(input_line cl: inputLines){

    if (cl.cflSize>4) {
      continue;
    }

    bagid id = make_pair(cl.cflSize,cl.opID);
    baginfo &b = fromid[id];
    cflSizes[id.first].insert(id);

    if(cl.sechar=='S'){
      if(b.start==nostart){ // az elsot vesszuk
        b.start=cl.time;
      }
    } else {
      assert(cl.sechar=='E');
      b.end=cl.time; // az utolsot vesszuk (mindig felulirunk)
    }

    if (cl.time<minTime)
      minTime=cl.time;
    if (cl.time>maxTime)
      maxTime=cl.time;
  }


  double y = 0;
  for(auto &e: cflSizes) {
    auto &bagids0 = e.second;

    // cflSize-ok kozti elvalasztovonal
    printf("  \\draw (0,%f) -- (8,%f);\n",y,y);
    y+=yScale/2;

    vector<bagid> bagids;
    for(const bagid &bid: bagids0){
      bagids.push_back(bid);
    }
    sort(bagids.begin(), bagids.end(), [](const bagid &a, const bagid &b) -> bool {
      int aind = -1, bind = -1;
      for(int i=0; i<(int)ops.size(); i++) {
        if(a.second==ops[i].id){
          assert(aind==-1);
          aind=i;
        }
        if(b.second==ops[i].id){
          assert(bind==-1);
          bind=i;
        }
      }
      assert(aind!=-1 && bind!=-1);
      return aind < bind;
    });

    // bal szoveg
    if(e.first>1) {
      printf("  \\node[anchor=east] at (0,%f) {\\small Step %d};\n",
        y+yScale*0.5*bagids.size()+yScale*1.5, e.first-1);
    }

    // vegigmegyunk a bageken
    set<int> opids;
    for(const bagid &bid: bagids){
      baginfo b = fromid[bid];
      int cflSize = bid.first;
      int opid = bid.second;

      assert(opids.count(opid)==0);
      opids.insert(opid);

      if (b.end != 0) {
        assert(b.start != 0);
        assert(b.start <= b.end);
        //printf("  \\node[anchor=east] at (0,%f) {\\tiny %d};\n",y+yScale/2,opid); // bal szoveg
        printf("  \\fill [mycolor%d] (%f,%f) rectangle (%f,%f);\n",
            color[opid], scale(b.start), y, scale(b.end), y+yScale);
        y+=yScale*3/2;
      }
    }
  }

  printf("  \\draw (0,%f) -- (8,%f);\n",y,y); // utolso elvalasztovonal


  printf(R"(
  \begin{axis}[
    hide axis,
    xmin=10,
    xmax=50,
    ymin=0,
    ymax=0.4,
    legend columns=2,
    legend style={font=\small, at={(0.6,-.05)}, anchor=north, row sep=-2pt,},
    ])");
  for(boperator &op: ops){
    printf(R"(
    \addlegendimage{mycolor%d,line width=0.8mm}
    \addlegendentry{%s};)", op.color, op.name.c_str());
  }
  printf("\n");
  printf(R"(  \end{axis})");
    

  return 0;
}
