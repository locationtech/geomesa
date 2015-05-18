/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.compute.spark

import java.io.{Serializable => JSerializable}
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.joda.time.{DateTime, DateTimeZone}
import org.junit
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory
import org.locationtech.geomesa.core.index.Constants
import org.locationtech.geomesa.feature.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class GeoMesaSparkTest extends Specification with Logging {

  sequential

  val testData : Map[String,String] = Map(
    "[MULTIPOLYGON] test box" -> "MULTIPOLYGON(((0.0 0.0,3.0 0.0,3.0 10.0,0.0 10.0,0.0 0.0)),((8.0 0.0,10.0 0.0,10.0 10.0,8.0 10.0,8.0 0.0)))",
    "[LINE] test line" -> "LINESTRING(0.0 0.0,10.0 10.0)",
    "[LINE] Cherry Avenue segment" -> "LINESTRING(-78.5000092574703 38.0272986617359,-78.5000196719491 38.0272519798381,-78.5000300864205 38.0272190279085,-78.5000370293904 38.0271853867342,-78.5000439723542 38.027151748305,-78.5000509153117 38.027118112621,-78.5000578582629 38.0270844741902,-78.5000648011924 38.0270329867966,-78.5000648011781 38.0270165108316,-78.5000682379314 38.026999348366,-78.5000752155953 38.026982185898,-78.5000786870602 38.0269657099304,-78.5000856300045 38.0269492339602,-78.5000891014656 38.0269327579921,-78.5000960444045 38.0269162820211,-78.5001064588197 38.0269004925451,-78.5001134017528 38.0268847030715,-78.50012381616 38.0268689135938,-78.5001307590877 38.0268538106175,-78.5001411734882 38.0268387076367,-78.5001550593595 38.0268236046505,-78.5001654737524 38.0268091881659,-78.5001758881429 38.0267954581791,-78.5001897740009 38.0267810416871,-78.50059593303 38.0263663951609,-78.5007972751677 38.0261625038609)",
    "[MULTILINE] Cherry Avenue entirety" -> "MULTILINESTRING((-78.5000092574703 38.0272986617359,-78.5000196719491 38.0272519798381,-78.5000300864205 38.0272190279085,-78.5000370293904 38.0271853867342,-78.5000439723542 38.027151748305,-78.5000509153117 38.027118112621,-78.5000578582629 38.0270844741902,-78.5000648011924 38.0270329867966,-78.5000648011781 38.0270165108316,-78.5000682379314 38.026999348366,-78.5000752155953 38.026982185898,-78.5000786870602 38.0269657099304,-78.5000856300045 38.0269492339602,-78.5000891014656 38.0269327579921,-78.5000960444045 38.0269162820211,-78.5001064588197 38.0269004925451,-78.5001134017528 38.0268847030715,-78.50012381616 38.0268689135938,-78.5001307590877 38.0268538106175,-78.5001411734882 38.0268387076367,-78.5001550593595 38.0268236046505,-78.5001654737524 38.0268091881659,-78.5001758881429 38.0267954581791,-78.5001897740009 38.0267810416871,-78.50059593303 38.0263663951609,-78.5007972751677 38.0261625038609),(-78.5028523013144 38.0243604125841,-78.5029182561703 38.0243350105182,-78.5029599119101 38.0243205930085,-78.5029980963034 38.0243068620707,-78.5030779364612 38.0242807730671,-78.5032133180745 38.0242505635318),(-78.5007972751677 38.0261625038609,-78.5012173130024 38.025738244047,-78.5014846420529 38.02546913345),(-78.5014846420529 38.02546913345,-78.5016408173938 38.0253105501453,-78.5017588424505 38.0251959031322,-78.5018733957502 38.0250812285921,-78.5019914201064 38.0249672952982,-78.5021094441212 38.024854020922,-78.5021337430596 38.0248279335242,-78.5021615133818 38.0248025325576,-78.5021858123053 38.0247771316478,-78.5022135826113 38.0247524171677,-78.5022413528989 38.0247277026809,-78.502269123189 38.0247036746865,-78.5023003648576 38.0246803331177,-78.5023593767632 38.0246336500237,-78.5023906184182 38.024611681429,-78.5024183886838 38.0245897128963,-78.5024531016733 38.0245677442145,-78.5024843433189 38.0245471485931,-78.5025155849473 38.0245265529633,-78.5025502979257 38.0245059572511,-78.502581539567 38.0244867620621,-78.5026162525558 38.0244675091228,-78.5026891499195 38.0244311230449,-78.5027655186506 38.0243967990852,-78.5028037030413 38.0243810087123,-78.5028523013144 38.0243604125841),(-78.4969404056816 38.0284800833846,-78.4975895903523 38.0283421148715,-78.4979714625632 38.0282611151835,-78.4983949926673 38.0281677578702,-78.4987352046273 38.0280915579077),(-78.490993782089 38.0272784134596,-78.4911499870459 38.0273827729123,-78.4912957791224 38.027476833921,-78.4914450429972 38.0275715814956,-78.4915734792506 38.0276567164341),(-78.4925003144519 38.0282437343292,-78.4925523852384 38.0282670785298,-78.4925870993641 38.0282801241573,-78.492618342024 38.0282925077758,-78.4926495847667 38.0283041554598,-78.4926842990611 38.0283158280625,-78.4927190134333 38.0283268141568,-78.492750256337 38.0283371135319,-78.4930731013898 38.0284304964614,-78.4933751186572 38.028515636636,-78.4938402963052 38.0286433804125,-78.4939826273169 38.0286797446143,-78.494128429974 38.0287168227748,-78.4942742327232 38.0287545872547,-78.4944165640021 38.0287930378917,-78.494451279106 38.0288005909897,-78.494485994217 38.0288081440776,-78.4945207094357 38.0288143241593,-78.4945519530924 38.0288205040724,-78.4945866683721 38.0288259976367,-78.4946213837063 38.0288308046928,-78.4946560990941 38.0288349252408,-78.4946907798189 38.028838359279,-78.4947255299783 38.0288417933102,-78.4947602455215 38.0288438543337,-78.4947949610667 38.028845915347,-78.4948331482303 38.0288472900024,-78.4948678638734 38.0288479779984,-78.494902579564 38.0288479794862,-78.4949372952546 38.0288479809639,-78.4949720110376 38.0288466094353,-78.4950067267276 38.0288466108928,-78.4950414424176 38.0288466123402,-78.495079629767 38.0288452409245,-78.4951143455462 38.0288438693545,-78.4951490613687 38.0288418112763,-78.495170411625 38.0288401370869,-78.4951837772336 38.02883906669,-78.4952184930959 38.0288363220936,-78.4952566458955 38.0288322046275),(-78.4915734792506 38.0276567164341,-78.4917366286941 38.0277624485909,-78.4918824220269 38.0278585683647,-78.4920282157351 38.027954687957,-78.492174009675 38.0280521776182,-78.4921983083911 38.028071401161,-78.4922260787251 38.0280899384265,-78.4922538490725 38.0281084784315,-78.492281619505 38.0281263291856,-78.4923093899508 38.0281441799332,-78.4923406320174 38.0281613443987,-78.4923684026307 38.0281778221358,-78.4923996448647 38.0281936135892,-78.4924274502879 38.0282094048163,-78.4924586579016 38.0282245097537,-78.4925003144519 38.0282437343292),(-78.4902856576172 38.0268115389132,-78.4904661595601 38.0269268852315,-78.4906015358649 38.0270175137791,-78.4907403839863 38.0271081424395,-78.4908757609528 38.027198767924,-78.490993782089 38.0272784134596),(-78.4952566458955 38.0288322046275,-78.4953261125364 38.0288232828931,-78.495360793841 38.0288177922616,-78.495426789099 38.0288054378431,-78.4954615052288 38.0287978876893,-78.4960030759596 38.0286798293809,-78.4962391448211 38.0286262901994,-78.4969404056816 38.0284800833846),(-78.488966633884 38.0258873969823,-78.4891228281985 38.0260253975831,-78.4893831600733 38.0262080297014,-78.4896469647716 38.026389975042,-78.4899072993849 38.0265712329949,-78.4900496176674 38.026659116773,-78.4901884646334 38.0267483703443,-78.4902856576172 38.0268115389132),(-78.4888597973336 38.0257928149758,-78.488966633884 38.0258873969823),(-78.5101312788355 38.020930264319,-78.5102839625207 38.0205732711847,-78.5104748140153 38.0201194779227,-78.5106205652871 38.0198551624104,-78.5106344459809 38.0198277011593,-78.5106483273497 38.0198050454089,-78.5106691480632 38.0197617940248,-78.5106795580672 38.0197377655797,-78.5106830270523 38.0197226622603),(-78.5097842230447 38.0214211408033,-78.5098709932951 38.0213428699136,-78.5098883471235 38.021325705977,-78.5099057008528 38.0213078555378,-78.5099230544826 38.021289318596,-78.5099404081038 38.0212707816515,-78.5099542905008 38.02125224774,-78.509971644014 38.0212330242907,-78.509985526304 38.0212138011293,-78.509999408495 38.021193891466,-78.5100132905866 38.021173295301,-78.5100237015513 38.0211533859259,-78.5100375836286 38.0211327897578,-78.5100479943964 38.0211115073801,-78.5101312788355 38.020930264319),(-78.509326429128 38.0217944686582,-78.5097842230447 38.0214211408033),(-78.5065492485878 38.0237719370643,-78.5065943737962 38.0237479071012,-78.5066290855314 38.0237300561942,-78.5066603258586 38.0237115189713,-78.5066915661702 38.0236929817402,-78.5067228063427 38.0236730715019,-78.5067540465606 38.0236538477547,-78.5070699172241 38.0234410146365,-78.507382314983 38.0232302403788,-78.5077433065893 38.0229947481592,-78.5080140488921 38.0228189863889,-78.5080834696148 38.0227722997325,-78.5081528902506 38.0227256130349,-78.5082223106483 38.0226775505509,-78.5082882596769 38.0226294910127,-78.5084409830377 38.0225148376076,-78.5085728791014 38.0224070477514,-78.5088505540671 38.0221825393168,-78.509326429128 38.0217944686582),(-78.5043415034512 38.0241626559166,-78.5043831595454 38.024160594896,-78.5051329697906 38.0241372240259,-78.5052405813537 38.0241331003531,-78.5053516642054 38.0241282899285,-78.5054592756933 38.0241234795593,-78.5055911864062 38.0241159219599,-78.5056154856949 38.0241138613212,-78.5056397849823 38.0241118006775,-78.5056606128154 38.0241083671949,-78.5056849119952 38.0241049335434,-78.5057057397719 38.0241008135537,-78.5057265675463 38.0240966935604,-78.5059140149657 38.0240287182419,-78.506129232103 38.0239525087043,-78.5062507595161 38.0239017014502,-78.5063930447698 38.0238440279676,-78.5064277568933 38.0238296096165,-78.5064937095269 38.0237980270754,-78.5065492485878 38.0237719370643),(-78.5032133180745 38.0242505635318,-78.5032584453345 38.0242423243172,-78.5033035725845 38.0242340850855,-78.5033486998554 38.024226532336,-78.5033938271483 38.0242196660686,-78.5034285404352 38.0242141730823,-78.5034389544329 38.0242127997841,-78.5034875531266 38.0242073063797,-78.5035326804601 38.0242018130583,-78.5035778078527 38.0241976927183,-78.5036229352404 38.0241935723613,-78.5036715340093 38.0241901383797,-78.5037166614555 38.0241873909858,-78.5041506143037 38.0241709006996,-78.5043415034512 38.0241626559166),(-78.4987352046273 38.0280915579077,-78.4987629770009 38.0280853797143,-78.4988636517995 38.0280640992688,-78.4989157249487 38.0280530910668,-78.4990163997051 38.0280284027148,-78.4990650013019 38.0280153623839,-78.49916220446 38.0279879031741,-78.4992142775479 38.027973487064,-78.4992628790947 38.0279583844115,-78.4993114806282 38.0279425952407,-78.4993566106209 38.0279261195331,-78.4993843829211 38.0279151357065,-78.4994121552185 38.0279034653752,-78.4994399275125 38.027891108539,-78.4994642282732 38.0278787516807,-78.4994920005586 38.0278650218356,-78.4995163013023 38.0278526649667,-78.4995169192356 38.027852313482),(-78.4995169192356 38.027852313482,-78.499544073581 38.0278368756143,-78.4995683743188 38.0278224592396,-78.4995926750509 38.0278073563615,-78.4996135042567 38.0277915669688,-78.4996378049763 38.0277750910847,-78.4996621056852 38.0277586151955,-78.4996864063977 38.0277387068094,-78.4997107071 38.0277181119199,-78.4997315362733 38.0276975170175,-78.4997558369519 38.0276762356201,-78.4997766661031 38.0276549542112,-78.4998009667569 38.0276329835598,-78.4998217958864 38.0276103291459,-78.4998426250034 38.0275876500141,-78.4998599825995 38.02756433655,-78.4998808116931 38.0275409956268,-78.4998946977563 38.027519714189,-78.4999085838116 38.0274984327495,-78.4999224698597 38.0274764922697,-78.4999363558997 38.0274544968681,-78.4999432989171 38.0274428263983,-78.4999502419314 38.0274325289248,-78.4999606564532 38.0274098744801,-78.4999710709686 38.0273872200343,-78.4999814854776 38.0273645655874,-78.4999918999804 38.0273419111395,-78.5000023144768 38.0273185701919,-78.5000092574703 38.0272986617359),(-78.4995169192356 38.027852313482,-78.4995641309868 38.027867842256,-78.4996028531833 38.0278762156112,-78.499653086986 38.0278796098155,-78.4997536240732 38.0278750709385,-78.4999020378101 38.0278615217351,-78.499997516849 38.0278502144613))",
    "[POLYGON] Charlottesville" -> "POLYGON((-78.4480347940116 38.0574871002506,-78.4478073388056 38.0570613767181,-78.447489602492 38.0556330888149,-78.4475613331476 38.0543729966714,-78.4474913359404 38.0531688593731,-78.4471033311625 38.0510124710374,-78.4468559007404 38.0503403123925,-78.4465022444764 38.049724095861,-78.4463611629831 38.0489399478115,-78.4466451302342 38.0479879914622,-78.4468935521083 38.047204016976,-78.4471774068971 38.0463920759449,-78.4473194556308 38.0458600786071,-78.4478506660003 38.0457483035168,-78.4481201134084 38.0456589845029,-78.4483408857566 38.0455080790522,-78.4489555764892 38.045281031607,-78.4495205621204 38.0452264072598,-78.4500656308001 38.0453050341156,-78.4515619871487 38.0456975562908,-78.4520278127326 38.0457369294639,-78.4525531004782 38.0457528189189,-78.4530387390881 38.0457608369821,-78.4539307629378 38.0458003696298,-78.4544066445935 38.0455810939866,-78.4546049904878 38.0454008962847,-78.4547042622303 38.0451187575263,-78.45477390994 38.0446720136184,-78.454883347649 38.0439980001295,-78.4547547718563 38.0435355014301,-78.4546195521738 38.0432666188877,-78.4544509624837 38.0428886257244,-78.4541980272186 38.0424661425413,-78.4538609688581 38.0417545844943,-78.4538050917856 38.0412654764507,-78.4538055456975 38.0405318605387,-78.4538903299325 38.0398649219979,-78.4540878941475 38.0387311822199,-78.4545383580856 38.038042167862,-78.4548198857144 38.0375532151034,-78.4552983572464 38.0369753656226,-78.4559737270544 38.0363753444128,-78.456311483729 38.0358863811039,-78.4569586439765 38.0352863437544,-78.4578027512843 38.0346864274753,-78.4584497458772 38.0345310119336,-78.4587028468452 38.0343976997343,-78.4589280721178 38.0341310048098,-78.4593782143373 38.0337087710827,-78.4597159233878 38.0333087402011,-78.4599973635754 38.0328863920337,-78.4601101325906 38.0324196117035,-78.4602229831227 38.0317971610709,-78.4604555397948 38.0307727369184,-78.4604892247959 38.0304259569015,-78.4606033941186 38.030055807579,-78.4610596435138 38.0293740452038,-78.4612745012046 38.0287701341041,-78.4611528322604 38.0282181499898,-78.4610469607118 38.0279985183977,-78.4605625005067 38.0277511374944,-78.460140841638 38.0275064392059,-78.4598315088329 38.0274173927489,-78.4594377589672 38.0274395021251,-78.4591002192831 38.0276394325831,-78.4587063573619 38.027861667526,-78.4583044761852 38.0280797244402,-78.4580512082664 38.0282998918073,-78.4578485323197 38.0286201679383,-78.4578059888932 38.0287728301014,-78.4575527254272 38.0290395134682,-78.4572433152976 38.0292616910978,-78.4569057267536 38.0294616701211,-78.4565316621609 38.0295807928969,-78.4563291050185 38.0296808378124,-78.4559240253018 38.0297607894811,-78.4555190271487 38.0297606392271,-78.455063402285 38.0297204319888,-78.4545064867333 38.0297202210031,-78.4542281271579 38.0295599134104,-78.4538992023681 38.0293195125074,-78.4535702921431 38.0290590923964,-78.4533098174528 38.028895878825,-78.4529346449186 38.0285099743953,-78.452579576467 38.0281240492591,-78.4524190060142 38.0275979353443,-78.4524326194999 38.0272628471103,-78.4525485912018 38.0265699140724,-78.4527462175347 38.0253694706732,-78.45302804476 38.0243469178326,-78.4539571672297 38.0225243215814,-78.45446402034 38.0214796357505,-78.4566010541373 38.0213470039148,-78.4569946318887 38.0215694625687,-78.4576715632512 38.0218640483203,-78.4582210821412 38.0221370295646,-78.4587572923289 38.0223164474586,-78.4593404019826 38.022495906379,-78.4603190447203 38.0226520987013,-78.4610966810329 38.0226523547397,-78.4618609491686 38.0225240063101,-78.4620689327232 38.0222123467767,-78.4625586494422 38.0214682256788,-78.4630953023495 38.0207631101049,-78.4636183940394 38.0202995810012,-78.4669173627013 38.0182586963005,-78.4683459993248 38.0162678737613,-78.4690301031858 38.015328974238,-78.4690972794554 38.0150445328578,-78.4689097864308 38.0147788080137,-78.4681435065681 38.0147112463513,-78.4695068845434 38.0121518287155,-78.4697058077266 38.0115231804633,-78.4720476723718 38.0120760020776,-78.4722822297623 38.012165686542,-78.4728652806575 38.0124736481891,-78.4732539371793 38.0126802632721,-78.4735019203215 38.0127972161935,-78.4739241493419 38.0128869389348,-78.4742793401881 38.0130272821437,-78.4744401479851 38.0132182466607,-78.4746344135635 38.0136196446557,-78.4746147418369 38.014106535084,-78.4746145815649 38.0145780793959,-78.4749267443022 38.0147128644932,-78.4751367221966 38.0147497873009,-78.4753511822194 38.0148784272864,-78.4755790828017 38.0148784741459,-78.4759879984775 38.0147499616421,-78.4765377258092 38.0145162759772,-78.4768796136275 38.0143916465332,-78.4772348718139 38.0143527489991,-78.4774812482407 38.0141969323815,-78.4780459914984 38.0140957099536,-78.4785030769381 38.0138378606924,-78.4787585686047 38.0135909030511,-78.4790140743559 38.0134113318813,-78.4793130472011 38.0133088206864,-78.4792979294484 38.0132541999618,-78.4794114855215 38.0131643976797,-78.4795533919009 38.0130970624964,-78.4797237077311 38.0129623723861,-78.4801211081832 38.0127378981307,-78.4805752656698 38.0124685070802,-78.4812504705776 38.0120778030625,-78.4815588636228 38.0118830217514,-78.48193421173 38.0118090467528,-78.4823634166166 38.0117726988772,-78.4827040194005 38.0117053891172,-78.4829594284038 38.0117278882263,-78.483158044834 38.0119524576319,-78.4831863852978 38.0121770028158,-78.4831579435691 38.0124015399732,-78.4831862993666 38.0125587255253,-78.4831578676165 38.0127383654107,-78.483186208378 38.01296291053,-78.48315777648 38.0131425229287,-78.4832428456305 38.0134568979172,-78.4831576448324 38.0137263242109,-78.4832143676419 38.0139957979267,-78.4832710511268 38.0142877064616,-78.4832993576695 38.0146694328523,-78.483294176284 38.0148681057083,-78.4835689419438 38.0151487033349,-78.4839911545882 38.0154332195007,-78.4845608651763 38.0156515196597,-78.4850635625441 38.0158308154324,-78.4856802143406 38.0160101502648,-78.486216428314 38.0160998155625,-78.4864644572302 38.0161388097697,-78.486705731891 38.0162947276079,-78.4870475493854 38.016524660737,-78.4872888351376 38.0166298861277,-78.4874832092389 38.0166416046549,-78.4878451980971 38.0166533400566,-78.4881895268302 38.0166197635353,-78.4885751882535 38.0166293297195,-78.4892139373057 38.0167628724533,-78.4895513766294 38.0168487151201,-78.4897200828032 38.016991741685,-78.4899731548683 38.0172492011166,-78.4902744340558 38.017296896683,-78.4906962792789 38.0172111176902,-78.490997615321 38.0170776575388,-78.4911904373362 38.01702047266,-78.4915038313264 38.0167535288296,-78.4919555597132 38.0166463011954,-78.492296169162 38.0163094987113,-78.4927787035232 38.0159727313467,-78.492977396659 38.0158155618519,-78.4970928967327 38.013682580216,-78.4984835788185 38.0131437019278,-78.4991931234303 38.0130538869042,-78.4994485433345 38.0128967342962,-78.4998437664113 38.0129287539271,-78.5003464802363 38.0126988302277,-78.5005877707069 38.0125936296879,-78.5008022656036 38.0124650328431,-78.5012178138321 38.0117558449659,-78.5014121704333 38.0113973512408,-78.5019743859181 38.0109207457232,-78.5022865703558 38.0106512741263,-78.5030244315817 38.0102919970931,-78.5035920201411 38.0100674400171,-78.5038474587522 38.009977610021,-78.5042163882422 38.0098878029786,-78.5069125259747 38.0106735581698,-78.5075652923892 38.0109429571282,-78.5079058876806 38.011055219413,-78.5082180759887 38.0110103010495,-78.5084167740383 38.0113021880897,-78.5084164376538 38.0113970627614,-78.5086443262663 38.0113853484664,-78.5089191301338 38.0112956984722,-78.5091134637878 38.0110618884754,-78.509327925494 38.0108202779,-78.5097221726638 38.0106060026718,-78.5100910435866 38.0101119921141,-78.510172326888 38.0098538326757,-78.5103666782886 38.009803179494,-78.510561032726 38.0097758670937,-78.5107554239778 38.0097641791796,-78.5109028585516 38.0097641657985,-78.5110637229321 38.0097524529809,-78.5113116930953 38.0096861959273,-78.5115060839921 38.0096744793298,-78.5118143923223 38.0097251131168,-78.51208919889 38.0097133875798,-78.5123974948409 38.0096860880482,-78.5128532757285 38.0097522736227,-78.5133425765796 38.0097755884693,-78.5137916582726 38.0098301006569,-78.5140195624705 38.0099313744627,-78.5142474606026 38.0099976087244,-78.5144284247841 38.0100092575271,-78.5147233296044 38.0100092215012,-78.5148841908108 38.0099819336493,-78.5151590002155 38.0099818990516,-78.5154002931798 38.0100325320629,-78.5155678583104 38.0100325103175,-78.5159500924788 38.0099007611223,-78.5161040543046 38.0100714050544,-78.5162783374999 38.0101610112144,-78.5165866805846 38.0101882366411,-78.51686818656 38.0101764991678,-78.5171564025364 38.0102154239737,-78.517338388399 38.0102882218848,-78.5175629930014 38.0103528026774,-78.5177127478026 38.0104725340821,-78.5179523654401 38.0104703289214,-78.5183213079293 38.0104927077501,-78.5186051419301 38.0106274105848,-78.5189457466686 38.0107171515413,-78.5192052153416 38.0107061260168,-78.5193582269777 38.0109250681797,-78.5193364314505 38.0111958832601,-78.5192491003715 38.011437903255,-78.5188340352807 38.011587791796,-78.518790383266 38.0117664266146,-78.5187904513964 38.0120372380663,-78.5188542773856 38.0123051012037,-78.5187978170053 38.0124809096119,-78.5187978619625 38.0126595374826,-78.5188197803747 38.0128669675239,-78.518834398527 38.0130282932169,-78.5187702286445 38.0133277280702,-78.5187421123125 38.0135945346255,-78.5188264867472 38.0138612959925,-78.518967153063 38.0142392341728,-78.5190187020363 38.0146727106298,-78.5189816270339 38.0150600682197,-78.5189321045338 38.0153640864355,-78.5189570312897 38.0157955623766,-78.5189942576665 38.016001479411,-78.5190718123973 38.0163250837603,-78.5192082876974 38.0168497135523,-78.51928587209 38.0172811807012,-78.5196392464914 38.0176341493337,-78.51974439766 38.0176838071893,-78.5197818465788 38.0178008356633,-78.5199740272022 38.0180165571445,-78.520141468358 38.0182626532725,-78.5202407303894 38.0183509753966,-78.5206129061057 38.0186256492796,-78.5206187989933 38.0187224997777,-78.5207121970512 38.0189396374862,-78.5212332036055 38.0190376605846,-78.521369581847 38.0189100844276,-78.5215555623747 38.0186647231039,-78.5216547924628 38.0186352679885,-78.5219028686934 38.0186254194828,-78.522212909395 38.0185174168012,-78.5223492903543 38.0182916973492,-78.522510492367 38.0181935523554,-78.5227214795118 38.018586026041,-78.5228456143743 38.0188411059911,-78.5229200980795 38.0190471517333,-78.5233544962418 38.0200087173655,-78.5233919406358 38.0207544415267,-78.5233920266994 38.0210292063194,-78.5235078546165 38.0213402779211,-78.5237397828542 38.022373441787,-78.5237706683738 38.0225640079623,-78.5234316582873 38.0231006980395,-78.5233106534283 38.0235606764049,-78.5233351467658 38.0244039399456,-78.523214228567 38.0251513963273,-78.5229980644635 38.0263906785026,-78.5200486370255 38.0260951647006,-78.520227651243 38.0285005977381,-78.5197027539389 38.0285063148315,-78.5197571739915 38.0285849785253,-78.5204409624135 38.0297112143958,-78.5201563565788 38.0303420169721,-78.5199075279322 38.030411065485,-78.5200174522885 38.0309744968867,-78.5202288921318 38.0330812115492,-78.5168795599589 38.0368524653907,-78.5156722277629 38.0368556506035,-78.5169224544723 38.0399869709215,-78.5162065004802 38.0404224180538,-78.5173086846758 38.0411645841191,-78.5168088996771 38.0422223517956,-78.5159817434742 38.04346924924,-78.5138958425554 38.0441253045007,-78.5145429779185 38.0448723526332,-78.507615482469 38.0562477092103,-78.5011431310711 38.0576531933158,-78.5009060110943 38.0584962880908,-78.5024471981014 38.0605196549688,-78.4949477146895 38.0652055763605,-78.4935526583464 38.0642469511724,-78.4925801543764 38.0635844877425,-78.492157628492 38.0630467200575,-78.4914220549693 38.0614749931234,-78.4912323721064 38.0616248537584,-78.4913034335911 38.0619058528612,-78.4868925118207 38.066889139645,-78.486513103012 38.0671700907363,-78.4851136285658 38.0690434872368,-78.4805851960675 38.0666634146305,-78.4802984729844 38.0657417981749,-78.4788228617472 38.0655700882117,-78.4774278973046 38.0649697592028,-78.4761327734651 38.0667658280819,-78.4745427740881 38.0676149601349,-78.4748174301878 38.0686982688173,-78.4729253470527 38.0700538600024,-78.4725295078174 38.0702446954685,-78.4717311577653 38.0705250627835,-78.4709935743572 38.0697689534562,-78.4722083419684 38.0680235864848,-78.4714503964357 38.0679766441182,-78.4712491169607 38.0681480530514,-78.4695137245745 38.0683514820538,-78.4695249339711 38.0679499444058,-78.4682882003939 38.0681111118,-78.4681945631601 38.0676091543367,-78.4668418676903 38.0678226411344,-78.4662524286672 38.0668374753678,-78.4653139480169 38.0656097925943,-78.4644758509069 38.0648692093359,-78.465335243717 38.0630575427858,-78.4663085789463 38.0613277305874,-78.4664494470514 38.061327770253,-78.4673892136686 38.0595317061067,-78.4687248297256 38.0572798254838,-78.4698455345863 38.0556396474866,-78.4711807511563 38.0540501730135,-78.4724353916266 38.0526164977684,-78.4720860496955 38.0519420067951,-78.471652148566 38.0514699274272,-78.4715798628713 38.051341206666,-78.4714714256453 38.0511266382172,-78.4713991679905 38.0509264122871,-78.4713992611406 38.0506832841434,-78.4717790748334 38.0502706003002,-78.4720943686619 38.0499394554518,-78.4723560322113 38.0496394915541,-78.4724164669572 38.0494173839294,-78.4723360778499 38.0492303372208,-78.4722499477388 38.0489815322078,-78.4718600453255 38.0489146039865,-78.4713772585177 38.0488365585299,-78.4710017199293 38.0488052736595,-78.4705680597292 38.0486664959493,-78.4703872918519 38.0484662149579,-78.4701343302694 38.0478368848681,-78.4699356469289 38.0472075403136,-78.4698289155783 38.0470086499745,-78.4695339345729 38.0468371439102,-78.4693053388149 38.0469393179596,-78.468407168336 38.0466034706965,-78.4676917660222 38.0462473734836,-78.4661192632977 38.045714987292,-78.4653292114835 38.047182785812,-78.4651074150484 38.048250382979,-78.4647453057632 38.0482502765916,-78.4642891032747 38.048663190151,-78.4639468173163 38.0492436680877,-78.4638729309242 38.0495125312664,-78.4573545185689 38.0497324748685,-78.4570999398137 38.0492609003817,-78.4571048708452 38.0499860033047,-78.4572469354638 38.0504060779264,-78.458898424969 38.0510299931243,-78.4567841773194 38.0556302795318,-78.4561273995112 38.0553332011668,-78.4557521877327 38.0549991258425,-78.4555445617933 38.0547458706718,-78.4552967746776 38.0545217622445,-78.4550136860866 38.0541576231591,-78.4547304813727 38.053989517875,-78.4542703578379 38.053569291964,-78.454093163388 38.053765231087,-78.4543764171386 38.0538493662526,-78.4544118048045 38.0539053700334,-78.4546595354771 38.0541574891887,-78.4541635943456 38.0544373334645,-78.4548714741119 38.0550816694308,-78.4492025035279 38.0589437591411,-78.4480347940116 38.0574871002506))"
  )

  val TEST_TABLE_NAME = "geomesa_spark_test"

  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
  lazy val dsParams = Map[String, String](
    zookeepersParam.key -> "dummy",
    instanceIdParam.key -> "dummy",
    userParam.key       -> "user",
    passwordParam.key   -> "pass",
    tableNameParam.key  -> TEST_TABLE_NAME,
    mockParam.key       -> "true",
    featureEncParam.key -> "avro")

  lazy val ds: DataStore = {
    val mockInstance = new MockInstance("dummy")
    val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
    c.tableOperations.create(TEST_TABLE_NAME)
    val splits = (0 to 99).map(s => "%02d".format(s)).map(new Text(_))
    c.tableOperations().addSplits(TEST_TABLE_NAME, new java.util.TreeSet[Text](splits.asJava))

    val dsf = new AccumuloDataStoreFactory

    val ds = dsf.createDataStore(dsParams.mapValues(_.asInstanceOf[JSerializable]).asJava)
    ds
  }

  lazy val spec = "id:Integer,map:Map[String,Integer],dtg:Date,geom:Geometry:srid=4326"

  def createFeatures(ds: DataStore, sft: SimpleFeatureType, encodedFeatures: Array[_<:Array[_]]): Seq[SimpleFeature] = {
    val builder = ScalaSimpleFeatureFactory.featureBuilder(sft)
    val features = encodedFeatures.map {
      e =>
        val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
        f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
        f
    }
    features
  }

  def createTypeName() = s"sparktest${UUID.randomUUID().toString}"
  def createSFT(typeName: String) = {
    val t = SimpleFeatureTypes.createType(typeName, spec)
    t.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    t
  }

  val randomSeed = 83

  var sc: SparkContext = null

  @junit.After
  def shutdown(): Unit = {
    Option(sc).foreach(_.stop())
  }

  "GeoMesaSpark" should {
    val random = new Random(randomSeed)
    val encodedFeatures = (0 until 150).toArray.map {
      i =>
        Array(
          i.toString,
          Map("a" -> i, "b" -> i * 2, (if (random.nextBoolean()) "c" else "d") -> random.nextInt(10)).asJava,
          new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate,
          "POINT(-77 38)")
    }

    "Read data" in {
      val typeName = s"sparktest${UUID.randomUUID().toString}"
      val sft = createSFT(typeName)

      ds.createSchema(sft)
      ds.getSchema(typeName) should not beNull
      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
      val feats = createFeatures(ds, sft, encodedFeatures)
      fs.addFeatures(DataUtilities.collection(feats.asJava))
      fs.getTransaction.commit()

      val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
      GeoMesaSpark.init(conf, ds)
      sc =  new SparkContext(conf) // will get shut down by shutdown method

      val rdd = GeoMesaSpark.rdd(new Configuration(), sc, dsParams, new Query(typeName), useMock = true)

      rdd.count() should equalTo(feats.length)
      feats.map(_.getAttribute("id")) should contain(rdd.take(1).head.getAttribute("id"))
    }

    "Write data" in {
      val typeName = s"sparktest${UUID.randomUUID().toString}"
      val sft = createSFT(typeName)
      ds.createSchema(sft)

      val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
      GeoMesaSpark.init(conf, ds)
      sc =  new SparkContext(conf) // will get shut down by shutdown method
      val feats = createFeatures(ds, sft, encodedFeatures)

      val rdd = sc.makeRDD(feats)

      GeoMesaSpark.save(rdd, dsParams, typeName)

      val coll = ds.getFeatureSource(typeName).getFeatures
      coll.size() should equalTo(encodedFeatures.length)
      feats.map(_.getAttribute("id")) should contain(coll.features().next().getAttribute("id"))
    }
  }
}
